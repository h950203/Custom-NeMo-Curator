from nemo_curator.core.client import RayClient
from nemo_curator.core.pipeline_helpers import analyze_removed_documents
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.filters.quality_filter import QualityFilterStage
from nemo_curator.stages.text.preprocessing.base_preprocess import preprocess_text
from nemo_curator.stages.text.preprocessing.korean_preprocessing import KoreanPreprocessing
from nemo_curator.stages.text.preprocessing.english_preprocessing import EnglishPreprocessing
from nemo_curator.stages.text.preprocessing.japanese_preprocessing import JapanesePreprocessing
from nemo_curator.stages.text.preprocessing.chinese_preprocessing import ChinesePreprocessing
from nemo_curator.stages.text.preprocessing.other_preprocessing import OtherPreprocessing
from nemo_curator.stages.text.analyze.syntax_analysis import SyntaxAnalysisStage
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
import time

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
    "ko": KoreanPreprocessing(),
    "en": EnglishPreprocessing(),
    "ja": JapanesePreprocessing(),
    "zh": ChinesePreprocessing(),
}
DEFAULT_PREPROCESSOR = OtherPreprocessing()

INPUT_PATH = "data/"
OUTPUT_PATH = "filtered_data/"
REMOVED_LOGS_PATH = "filtered_data_log/"
PIPELINE_VERSION = "v1.1-chunking"
CHUNK_SIZE = 100000  # 한 번에 처리할 문서의 수 (메모리 사용량과 직결)


def read_jsonl_in_chunks(file_path, chunk_size):
    """
    JSONL 파일을 읽어 지정된 크기의 청크(문서 리스트)로 반환하는 제너레이터.
    각 문서에 추적 ID를 추가하여 원본 위치를 기억합니다.
    """
    docs = []
    filename = os.path.basename(file_path)
    logger.info(f"'{filename}' 파일 청크 단위 읽기 시작 (청크 크기: {chunk_size})")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    doc = json.loads(line)
                    doc['_line_num'] = line_num
                    doc['_source_file'] = filename
                    doc['_tracking_id'] = f"{filename}::{line_num}"
                    docs.append(doc)

                    if len(docs) >= chunk_size:
                        yield docs
                        docs = []

                except json.JSONDecodeError:
                    logger.warning(f"JSON 파싱 오류 - {filename}:{line_num}")
                    continue
        
        if docs:
            yield docs

    except IOError as e:
        logger.error(f"파일 읽기 실패: {file_path} - {e}")
        raise

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
                        'original': original_input[:200],
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


def process_chunk(chunk_docs, chunk_id, temp_output_dir, config):
    """
    단일 청크를 처리하고, 결과를 임시 파일에 저장하며, 통계 및 로그를 반환.
    """
    
    if not chunk_docs:
        return {},
        {"error": "Empty chunk", "chunk_id": chunk_id},
        []

    first_doc = chunk_docs[0]
    filename = first_doc.get('_source_file', 'unknown_file')
    
    total_input = len(chunk_docs)
    all_input_ids = {doc['_tracking_id'] for doc in chunk_docs}
    tracked_output_ids = set()
    
    # 전처리 로그 생성
    local_prep_logs = process_preprocessing_logs(chunk_docs, LANG_CODE_MAP, PREPROCESSORS)
    
    # 임시 파일 경로 설정
    pid = os.getpid()
    temp_input_path = f"/tmp/temp_input_{{pid}}_{chunk_id}.jsonl"
    pipeline_output_path = f"/tmp/pipeline_output_{{pid}}_{chunk_id}/"
    
    output_docs = []
    removed_docs = {}
    stats = {}
    
    try:
        # 임시 입력 파일 작성
        with open(temp_input_path, 'w', encoding='utf-8') as f:
            for doc in chunk_docs:
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 파이프라인 생성 및 실행
        pipeline = Pipeline(name=f"filter_chunk_{chunk_id}")
        pipeline.add_stage(JsonlReader(file_paths=temp_input_path))
        pipeline.add_stage(QualityFilterStage(
            filter_config=config,
            lang_code_map=LANG_CODE_MAP,
            preprocessors=PREPROCESSORS
        ))
        pipeline.add_stage(SyntaxAnalysisStage())
        pipeline.add_stage(JsonlWriter(path=pipeline_output_path))
        
        executor = XennaExecutor()
        pipeline.run(executor)
        
        # 출력 파일 읽기
        output_files = glob.glob(os.path.join(pipeline_output_path, "*.jsonl"))
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try:
                        doc = json.loads(line)
                        output_docs.append(doc)
                        if '_tracking_id' in doc:
                            tracked_output_ids.add(doc['_tracking_id'])
                    except json.JSONDecodeError:
                        continue
        
        # 최종 청크 출력 파일 저장
        os.makedirs(temp_output_dir, exist_ok=True)
        chunk_output_path = os.path.join(temp_output_dir, f"part-{chunk_id}.jsonl")
        
        with open(chunk_output_path, 'w', encoding='utf-8') as f:
            for doc in output_docs:
                if 'processing_results' in doc:
                    doc['input'] = doc['processing_results'].get('processed_input')
                    doc['output'] = doc['processing_results'].get('processed_output')
                
                for key in ['_line_num', '_source_file', '_tracking_id', 'processing_results']:
                    doc.pop(key, None)
                
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 제거된 문서 분석
        removed_docs = analyze_removed_documents(
            chunk_docs, all_input_ids, tracked_output_ids, 
            LANG_CODE_MAP, PREPROCESSORS, config
        )

        total_output = len(output_docs)
        total_removed = sum(len(v) for v in removed_docs.values())
        
        stats = {
            "chunk_id": chunk_id,
            "filename": filename,
            "total_input": total_input,
            "total_output": total_output,
            "total_removed": total_removed,
            "breakdown": {k: len(v) for k, v in removed_docs.items() if v},
        }
        
    except Exception as e:
        logger.error(f"청크 {chunk_id} 처리 실패: {e}")
        traceback.print_exc()
        stats = {"error": str(e), "chunk_id": chunk_id, "filename": filename}
    
    finally:
        # 임시 파일 정리
        if os.path.exists(temp_input_path): os.remove(temp_input_path)
        if os.path.exists(pipeline_output_path): shutil.rmtree(pipeline_output_path)
    
    return removed_docs, stats, local_prep_logs


def main():
    ray_client = None
    try:
        # Ray 클러스터 초기화
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
        total_cpus = cpu_count()
        num_cpus = max(1, int(total_cpus * 0.75))
        
        logger.info(f"Ray 초기화 중: 전체 CPU={total_cpus}, 사용할 CPU={num_cpus}, GPU={num_gpus}")
        
        ray_client = RayClient(num_cpus=num_cpus, num_gpus=num_gpus)
        ray_client.start()

        print_filter_config(FILTER_CONFIG, PIPELINE_VERSION)
        logger.info(f"입력: {INPUT_PATH}, 출력: {OUTPUT_PATH}, 로그: {REMOVED_LOGS_PATH}")
        print("-" * 70)

        input_files = sorted(glob.glob(os.path.join(INPUT_PATH, "*.jsonl")))
        if not input_files:
            logger.error(f"{INPUT_PATH}에 jsonl 파일이 없습니다.")
            return
        
        logger.info(f"발견된 파일: {len(input_files)}개")

        # 전역 통계 및 로그
        global_all_stats = []
        global_removed_docs = defaultdict(list)
        global_preprocessing_logs = []

        # 각 파일을 순차적으로 처리 (파일 내 청크는 병렬로)
        for input_file in input_files:
            filename = os.path.basename(input_file)
            logger.info(f"'{filename}' 파일 처리 시작...")
            
            # 파일별 임시 출력 디렉토리
            file_temp_output_dir = f"/tmp/temp_outputs_{os.path.basename(input_file)}_{int(time.time())}"
            os.makedirs(file_temp_output_dir, exist_ok=True)

            @ray.remote(max_retries=1, memory=8 * 1024 * 1024 * 1024) # 8GB per task
            def process_chunk_remote(chunk_docs, chunk_id):
                return process_chunk(chunk_docs, chunk_id, file_temp_output_dir, FILTER_CONFIG)

            # 청크 생성 및 병렬 처리
            chunk_generator = read_jsonl_in_chunks(input_file, CHUNK_SIZE)
            futures = [process_chunk_remote.remote(chunk, f"{filename}-{i}") 
                       for i, chunk in enumerate(chunk_generator)]
            
            logger.info(f"'{filename}': {len(futures)}개 청크 생성 완료. 병렬 처리 시작.")
            
            file_stats = []
            
            with tqdm(total=len(futures), desc=f"Processing {filename}") as pbar:
                while futures:
                    done, futures = ray.wait(futures, num_returns=1, timeout=600)
                    if not done:
                        logger.warning("Ray.wait 타임아웃 발생")
                        continue

                    try:
                        removed_docs, stats, prep_logs = ray.get(done[0])
                        
                        if prep_logs: global_preprocessing_logs.extend(prep_logs)
                        
                        if "error" in stats:
                            logger.error(f"청크 처리 오류 ({stats['chunk_id']}): {stats['error']}")
                        else:
                            file_stats.append(stats)
                            for stage, docs in removed_docs.items():
                                global_removed_docs[stage].extend(docs)
                        
                        del removed_docs
                        del prep_logs

                    except Exception as e:
                        logger.error(f"결과 처리 중 오류: {e}")
                    
                    pbar.update(1)

            # 파일별 결과 집계 및 출력 파일 병합
            logger.info(f"'{filename}' 처리 완료. 결과 병합 중...")
            total_input_docs = sum(s['total_input'] for s in file_stats)
            total_output_docs = sum(s['total_output'] for s in file_stats)
            
            final_output_path = os.path.join(OUTPUT_PATH, filename)
            os.makedirs(OUTPUT_PATH, exist_ok=True)

            # 청크 결과 파일들을 최종 파일 하나로 병합
            with open(final_output_path, 'w', encoding='utf-8') as outfile:
                part_files = sorted(glob.glob(os.path.join(file_temp_output_dir, "*.jsonl")))
                for part_file in part_files:
                    with open(part_file, 'r', encoding='utf-8') as infile:
                        shutil.copyfileobj(infile, outfile)
            
            shutil.rmtree(file_temp_output_dir) # 임시 파일 정리
            
            logger.info(f"'{filename}' 최종 출력 완료: {final_output_path}")
            
            file_summary = {
                "filename": filename,
                "total_input": total_input_docs,
                "total_output": total_output_docs,
                "total_removed": total_input_docs - total_output_docs,
                "breakdown": {
                    stage: sum(s['breakdown'].get(stage, 0) for s in file_stats)
                    for stage in {s for stat in file_stats for s in stat.get('breakdown', {})}
                }
            }
            global_all_stats.append(file_summary)
            print("-" * 70)


        # 최종 전역 요약 출력
        print("\n" + "=" * 70)
        print("전체 요약")
        print("=" * 70)
        
        total_input_all = sum(s['total_input'] for s in global_all_stats)
        total_output_all = sum(s['total_output'] for s in global_all_stats)

        for stats in sorted(global_all_stats, key=lambda x: x.get('filename', '')):
            print(f"\n파일: {stats['filename']}")
            print(f"  입력: {stats['total_input']}, 출력: {stats['total_output']}, "
                  f"제거: {stats['total_removed']}")
            if stats.get('breakdown'):
                print("  제거 상세:")
                for stage, count in stats['breakdown'].items():
                    if count > 0: print(f"    {stage}: {count}개")
        
        print("\n" + "=" * 70)
        print(f"총 처리 파일: {len(global_all_stats)}개")
        print(f"총 입력 문서: {total_input_all}개")
        print(f"총 출력 문서: {total_output_all}개")
        print(f"총 제거 문서: {total_input_all - total_output_all}개")
        if total_input_all > 0:
            print(f"보존율: {total_output_all / total_input_all * 100:.2f}%")
        else:
            print("보존율: N/A")
        
        # 전역 로그 저장
        logger.info("전역 로그 저장 중...")
        save_global_logs(REMOVED_LOGS_PATH, global_removed_docs)
        save_preprocessing_logs(REMOVED_LOGS_PATH, global_preprocessing_logs)
        create_summary(REMOVED_LOGS_PATH, global_removed_docs, PIPELINE_VERSION)
        
        print("\n" + "=" * 70)
        print("✓ 모든 파일 처리 완료!")
        print("=" * 70)

    except Exception as e:
        logger.error(f"메인 프로세스 실패: {e}")
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
