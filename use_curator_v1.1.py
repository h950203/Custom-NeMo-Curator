from nemo_curator.core.client import RayClient
from nemo_curator.core.pipeline_helpers import analyze_removed_documents
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.filters.custom_filter import MultiLingualQualityFilterStage
from nemo_curator.stages.text.utils.custom_utils import preprocess_text
from nemo_curator.stages.text.preprocessing.korean_preprocessing import KoreanPersonalFilter
from nemo_curator.stages.text.preprocessing.english_preprocessing import EnglishPersonalFilter
from nemo_curator.stages.text.preprocessing.japanese_preprocessing import JapanesePersonalFilter
from nemo_curator.stages.text.preprocessing.chinese_preprocessing import ChinesePersonalFilter
from nemo_curator.stages.text.preprocessing.other_preprocessing import OtherPersonalFilter
from nemo_curator.stages.text.analyze.sentence_analysis import SentenceAnalysisStage  # 새 스테이지 import
from nemo_curator.utils.log_utils import (
    print_filter_config,
    save_global_logs,
    save_preprocessing_logs,
    create_summary,
)

import os
import json
import glob
from collections import defaultdict
import shutil
import traceback
import torch
import ray
from multiprocessing import cpu_count
from tqdm import tqdm
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# 필터링 기준 설정
# ============================================================================
FILTER_CONFIG = {
    'defaults': {
        'max_repeated_line_fraction': 0.7,
        'ngram_n': 5,
        'max_repeating_ngram_ratio': 0.2,
        'max_symbol_to_word_ratio': 0.8,
        'max_url_ratio': 0.01,
        'min_quality_score': 0.5,
    },
    'ko': {
        'min_words': 2,
        'max_words': 100000,
        'lang_char_pattern': r'[가-힣]',
        'lang_threshold': 0.7,
    },
    'en': {
        'min_words': 3,
        'max_words': 100000,
        'lang_char_pattern': r'[a-zA-Z]',
        'lang_threshold': 0.7,
    },
    'ja': {
        'min_words': 3,
        'max_words': 100000,
        'lang_char_pattern': r'[ぁ-んァ-ン一-龯]',
        'lang_threshold': 0.7,
    },
    'zh': {
        'min_words': 3,
        'max_words': 100000,
        'lang_char_pattern': r'[\u4e00-\u9fff]',
        'lang_threshold': 0.7,
    },
    'quality_weights': {
        'lang_ratio': 0.1,
        'repeated_lines': 0.25,
        'repeated_ngrams': 0.25,
        'symbols': 0.2,
        'urls': 0.2,
    }
}

# 언어 코드 매핑
LANG_CODE_MAP = {
    "korean": "ko",
    "english": "en",
    "japanese": "ja",
    "chinese": "zh",
}

# 언어별 전처리기
PREPROCESSORS = {
    "ko": KoreanPersonalFilter(),
    "en": EnglishPersonalFilter(),
    "ja": JapanesePersonalFilter(),
    "zh": ChinesePersonalFilter(),
}
DEFAULT_PREPROCESSOR = OtherPersonalFilter()

INPUT_PATH = "data/"
OUTPUT_PATH = "filtered_data_v1.1/"
REMOVED_LOGS_PATH = "filtered_data_v1.1_log/"
PIPELINE_VERSION = "v1.1"


def safe_read_jsonl(file_path):
    """안전하게 JSONL 파일을 읽고 유효한 문서만 반환"""
    valid_docs = []
    error_count = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # 빈 줄 건너뛰기
                line = line.strip()
                if not line:
                    continue
                
                try:
                    doc = json.loads(line)
                    doc['_line_num'] = line_num
                    doc['_source_file'] = os.path.basename(file_path)
                    doc['_tracking_id'] = f"{os.path.basename(file_path)}::{line_num}"
                    valid_docs.append(doc)
                except json.JSONDecodeError as e:
                    error_count += 1
                    logger.warning(f"JSON 파싱 오류 - {file_path}:{line_num} - {e}")
                    continue
                except Exception as e:
                    error_count += 1
                    logger.warning(f"문서 처리 오류 - {file_path}:{line_num} - {e}")
                    continue
    
    except IOError as e:
        logger.error(f"파일 읽기 실패: {file_path} - {e}")
        return None, str(e)
    
    if error_count > 0:
        logger.info(f"{file_path}: {error_count}개의 잘못된 라인을 건너뛰었습니다.")
    
    return valid_docs, None


def process_preprocessing_logs(docs, lang_code_map, preprocessors):
    """전처리 변경사항 로그 생성"""
    prep_logs = []
    
    for doc in docs:
        try:
            input_lang = lang_code_map.get(doc.get('src', '').lower()) if doc.get('src') else None
            output_lang = lang_code_map.get(doc.get('tgt', '').lower()) if doc.get('tgt') else None
            
            original_input = doc.get('input', '')
            if original_input:
                processed_input = preprocess_text(original_input, input_lang, preprocessors)
                if original_input != processed_input:
                    prep_logs.append({
                        '_source_file': doc.get('_source_file', 'unknown'),
                        '_line_num': doc.get('_line_num', 0),
                        'field': 'input',
                        'original': original_input[:200],  # 메모리 절약을 위해 200자로 제한
                        'modified': (processed_input or "[제거됨]")[:200]
                    })

            original_output = doc.get('output', '')
            if original_output:
                processed_output = preprocess_text(original_output, output_lang, preprocessors)
                if original_output != processed_output:
                    prep_logs.append({
                        '_source_file': doc.get('_source_file', 'unknown'),
                        '_line_num': doc.get('_line_num', 0),
                        'field': 'output',
                        'original': original_output[:200],
                        'modified': (processed_output or "[제거됨]")[:200]
                    })
        except Exception as e:
            logger.warning(f"전처리 로그 생성 중 오류: {e}")
            continue
    
    return prep_logs


def process_file_with_tracking(input_file, output_dir, config):
    """개별 파일을 처리하고 제거된 문서 추적 (개선된 버전)"""
    
    filename = os.path.basename(input_file)
    logger.info(f"처리 시작: {filename}")
    
    # 안전하게 파일 읽기
    all_docs, read_error = safe_read_jsonl(input_file)
    
    if read_error:
        return {}, {"error": read_error, "filename": filename}, []
    
    if not all_docs:
        logger.warning(f"{filename}: 유효한 문서가 없습니다.")
        return {}, {"error": "No valid documents", "filename": filename}, []
    
    total_input = len(all_docs)
    all_input_ids = {doc['_tracking_id'] for doc in all_docs}
    tracked_output_ids = set()
    
    logger.info(f"{filename}: {total_input}개 문서 로드 완료")

    # 전처리 로그 생성 (메모리 효율적으로)
    try:
        local_prep_logs = process_preprocessing_logs(all_docs, LANG_CODE_MAP, PREPROCESSORS)
    except Exception as e:
        logger.warning(f"전처리 로그 생성 실패: {e}")
        local_prep_logs = []
    
    # 임시 파일 경로
    temp_input_path = f"/tmp/temp_input_{os.getpid()}_{filename}"
    temp_output_path = f"/tmp/temp_output_{os.getpid()}_{filename}/"
    
    output_docs = []
    removed_docs = {}
    stats = {}
    
    try:
        # 임시 입력 파일 작성
        with open(temp_input_path, 'w', encoding='utf-8') as f:
            for doc in all_docs:
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 파이프라인 생성 및 실행
        pipeline = Pipeline(name=f"filter_{filename}")
        pipeline.add_stage(JsonlReader(file_paths=temp_input_path))
        pipeline.add_stage(MultiLingualQualityFilterStage(
            filter_config=config,
            lang_code_map=LANG_CODE_MAP,
            preprocessors=PREPROCESSORS
        ))
        pipeline.add_stage(SentenceAnalysisStage())  # 새 스테이지 추가
        pipeline.add_stage(JsonlWriter(path=temp_output_path))
        
        executor = XennaExecutor()
        pipeline.run(executor)
        
        # 출력 파일 읽기
        output_files = glob.glob(os.path.join(temp_output_path, "*.jsonl"))
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        doc = json.loads(line)
                        output_docs.append(doc)
                        if '_tracking_id' in doc:
                            tracked_output_ids.add(doc['_tracking_id'])
                    except json.JSONDecodeError:
                        continue
        
        # 최종 출력 파일 저장
        os.makedirs(output_dir, exist_ok=True)
        final_output_path = os.path.join(output_dir, filename)
        
        with open(final_output_path, 'w', encoding='utf-8') as f:
            for doc in output_docs:
                # 처리 결과 적용
                if 'processing_results' in doc:
                    doc['input'] = doc['processing_results'].get('processed_input')
                    doc['output'] = doc['processing_results'].get('processed_output')
                
                # 임시 필드 제거
                for key in ['_line_num', '_source_file', '_tracking_id', 'processing_results']:
                    doc.pop(key, None)
                
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 제거된 문서 분석
        removed_docs = analyze_removed_documents(
            all_docs, all_input_ids, tracked_output_ids, 
            LANG_CODE_MAP, PREPROCESSORS, config
        )

        total_output = len(output_docs)
        total_removed = sum(len(v) for v in removed_docs.values())
        missing = len(all_input_ids) - total_output - total_removed
        
        logger.info(f"{filename}: 출력={total_output}, 제거={total_removed}, 누락={missing}")

        stats = {
            "filename": filename,
            "total_input": total_input,
            "total_output": total_output,
            "total_removed": total_removed,
            "missing": missing,
            "breakdown": {k: len(v) for k, v in removed_docs.items() if v},
        }
        
    except Exception as e:
        logger.error(f"{filename} 처리 실패: {e}")
        traceback.print_exc()
        stats = {"error": str(e), "filename": filename}
    
    finally:
        # 임시 파일 정리
        try:
            if os.path.exists(temp_input_path):
                os.remove(temp_input_path)
            if os.path.exists(temp_output_path):
                shutil.rmtree(temp_output_path)
        except Exception as e:
            logger.warning(f"임시 파일 정리 실패: {e}")
    
    # 메모리 해제를 위해 큰 객체 반환 전에 정리
    del all_docs
    del output_docs
    
    return removed_docs, stats, local_prep_logs


def main():
    ray_client = None
    try:
        # Ray 클러스터 초기화
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
        num_cpus = cpu_count()
        
        logger.info(f"Ray 초기화 중: CPU={num_cpus}, GPU={num_gpus}")
        
        # Ray 메모리 설정 추가
        ray_client = RayClient(
            num_cpus=num_cpus,
            num_gpus=num_gpus
        )
        ray_client.start()

        # 현재 설정 출력
        print_filter_config(FILTER_CONFIG, PIPELINE_VERSION)

        logger.info("병렬 파일 처리 시작")
        logger.info(f"입력: {INPUT_PATH}")
        logger.info(f"출력: {OUTPUT_PATH}")
        logger.info(f"로그: {REMOVED_LOGS_PATH}")
        print("-" * 70)

        # 입력 파일 목록
        input_files = sorted(glob.glob(os.path.join(INPUT_PATH, "*.jsonl")))
        
        if not input_files:
            logger.error(f"{INPUT_PATH}에 jsonl 파일이 없습니다.")
            return
        
        logger.info(f"발견된 파일: {len(input_files)}개")

        # Ray 원격 함수 정의 (메모리 제한 추가)
        @ray.remote(max_retries=2, memory=4*1024*1024*1024)  # 4GB per task
        def process_file_remote(input_file):
            try:
                return process_file_with_tracking(
                    input_file, OUTPUT_PATH, FILTER_CONFIG
                )
            except Exception as e:
                logger.error(f"원격 함수 실행 오류: {e}")
                traceback.print_exc()
                return {}, {"error": str(e), "filename": os.path.basename(input_file)}, []

        # 병렬 처리 실행 (배치 처리로 메모리 관리)
        batch_size = min(num_cpus, 10)  # 동시 처리 파일 수 제한
        all_stats = []
        global_removed_docs = defaultdict(list)
        global_preprocessing_logs = []
        
        logger.info(f"배치 크기: {batch_size}")

        for i in range(0, len(input_files), batch_size):
            batch_files = input_files[i:i+batch_size]
            logger.info(f"배치 {i//batch_size + 1}/{(len(input_files)-1)//batch_size + 1} 처리 중...")
            
            futures = [process_file_remote.remote(f) for f in batch_files]
            
            with tqdm(total=len(futures), desc=f"Batch {i//batch_size + 1}") as pbar:
                while futures:
                    done, futures = ray.wait(futures, timeout=300)  # 5분 타임아웃
                    
                    for future in done:
                        try:
                            removed_docs, stats, prep_logs = ray.get(future, timeout=60)
                            
                            # 메모리 절약을 위해 즉시 처리
                            if prep_logs:
                                global_preprocessing_logs.extend(prep_logs)
                            
                            if "error" in stats:
                                logger.error(f"파일 처리 오류 ({stats['filename']}): {stats['error']}")
                            else:
                                all_stats.append(stats)
                                for stage_name, docs in removed_docs.items():
                                    global_removed_docs[stage_name].extend(docs)
                            
                            # 메모리 해제
                            del removed_docs
                            del prep_logs
                            
                        except ray.exceptions.RayTaskError as e:
                            logger.error(f"Ray 태스크 오류: {e}")
                        except Exception as e:
                            logger.error(f"결과 처리 중 오류: {e}")
                        
                        pbar.update(1)
        
        # 처리 결과 요약 출력
        print("\n" + "=" * 70)
        print("개별 파일 처리 결과 요약")
        print("=" * 70)
        
        total_input_docs = 0
        total_output_docs = 0
        
        for stats in sorted(all_stats, key=lambda x: x.get('filename', '')):
            print(f"\n파일: {stats['filename']}")
            print(f"  입력: {stats['total_input']}, 출력: {stats['total_output']}, "
                  f"제거: {stats['total_removed']}, 누락: {stats['missing']}")
            
            total_input_docs += stats['total_input']
            total_output_docs += stats['total_output']
            
            if stats.get('breakdown'):
                print("  제거 상세:")
                for stage, count in stats['breakdown'].items():
                    print(f"    {stage}: {count}개")
        
        print("\n" + "=" * 70)
        print("전체 요약")
        print("=" * 70)
        print(f"총 처리 파일: {len(all_stats)}개")
        print(f"총 입력 문서: {total_input_docs}개")
        print(f"총 출력 문서: {total_output_docs}개")
        print(f"총 제거 문서: {total_input_docs - total_output_docs}개")
        print(f"보존율: {total_output_docs/total_input_docs*100:.2f}%")
        
        # 전역 로그 저장
        logger.info("로그 저장 중...")
        save_global_logs(REMOVED_LOGS_PATH, global_removed_docs)
        save_preprocessing_logs(REMOVED_LOGS_PATH, global_preprocessing_logs)
        create_summary(REMOVED_LOGS_PATH, global_removed_docs, PIPELINE_VERSION)
        
        print("\n" + "=" * 70)
        print("✓ 모든 파일 처리 완료!")
        print("=" * 70)

    except Exception as e:
        logger.error(f"처리 실패: {e}")
        traceback.print_exc()

    finally:
        if ray_client:
            logger.info("Ray 클러스터 종료 중...")
            try:
                ray_client.stop()
                logger.info("Ray 종료 완료")
            except Exception as e:
                logger.warning(f"Ray 종료 중 오류: {e}")


if __name__ == "__main__":
    main()