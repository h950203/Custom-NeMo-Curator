"""
This script is a modification of use_curator_v1.2.py to process plain text (.txt) files.
It reads text files, processes each line for preprocessing and quality filtering,
and saves the refined text into a single output file.
"""

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
# 기본 설정
# ============================================================================
# 처리할 텍스트의 주 언어를 설정합니다. (e.g., 'ko', 'en', 'ja', 'zh')
LANGUAGE = 'ko'

# 입출력 경로 및 버전 설정
INPUT_DIR = "data_txt/"
OUTPUT_DIR = "filtered_data_txt_v1.0/"
OUTPUT_FILENAME = os.path.join(OUTPUT_DIR, "curated_output.txt")
REMOVED_LOGS_PATH = "filtered_data_txt_v1.0_log/"
PIPELINE_VERSION = "v1.0"

# ============================================================================
# 필터링 기준 설정 (use_curator_v1.2.py와 동일)
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

# 언어별 전처리기
PREPROCESSORS = {
    "ko": KoreanPreprocessing(),
    "en": EnglishPreprocessing(),
    "ja": JapanesePreprocessing(),
    "zh": ChinesePreprocessing(),
}
DEFAULT_PREPROCESSOR = OtherPreprocessing()

def safe_read_txt(file_path):
    """
    .txt 파일을 안전하게 읽어 각 줄을 문서 형식의 리스트로 변환합니다.
    """
    docs = []
    error_count = 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    doc = {
                        'text': line,
                        'lang': LANGUAGE, # 문서 전체에 단일 언어 적용
                        '_line_num': line_num,
                        '_source_file': os.path.basename(file_path),
                        '_tracking_id': f"{os.path.basename(file_path)}::{line_num}"
                    }
                    docs.append(doc)
                except Exception as e:
                    error_count += 1
                    logger.warning(f"문서 변환 오류 - {file_path}:{line_num} - {e}")
                    continue
    except IOError as e:
        logger.error(f"파일 읽기 실패: {file_path} - {e}")
        return None, str(e)

    if error_count > 0:
        logger.info(f"{file_path}: {error_count}개의 라인을 문서로 변환하지 못했습니다.")
        
    return docs, None


def analyze_removed_txt_documents(all_docs, all_input_ids, tracked_output_ids, config):
    """
    .txt 처리에 맞게 제거된 문서를 분석하는 간소화된 함수
    """
    removed_ids = all_input_ids - tracked_output_ids
    if not removed_ids:
        return {}

    # ID를 원래 문서에 빠르게 매핑
    doc_map = {doc['_tracking_id']: doc for doc in all_docs}
    
    removed_docs = defaultdict(list)
    for doc_id in removed_ids:
        doc = doc_map.get(doc_id)
        if not doc:
            continue

        # 품질 점수가 있다면 그것을 기준으로, 없다면 'unknown_filter'로 분류
        reason = doc.get('quality_score', {}).get('reason', 'unknown_filter')
        removed_docs[reason].append(doc)

    return removed_docs


def process_txt_file(input_file, config):
    """
    개별 .txt 파일을 처리하고, 필터링된 문서 리스트와 통계를 반환합니다.
    """
    filename = os.path.basename(input_file)
    logger.info(f"처리 시작: {filename}")
    
    # .txt 파일 읽어서 문서 리스트로 변환
    all_docs, read_error = safe_read_txt(input_file)
    
    if read_error:
        return {}, {"error": read_error, "filename": filename}, []
    
    if not all_docs:
        logger.warning(f"{filename}: 유효한 문서가 없습니다.")
        return {}, {"error": "No valid documents", "filename": filename}, []

    total_input = len(all_docs)
    all_input_ids = {doc['_tracking_id'] for doc in all_docs}
    
    logger.info(f"{filename}: {total_input}개 라인 로드 완료")
    
    # 임시 파일 경로
    temp_input_path = f"/tmp/temp_input_{os.getpid()}_{filename}.jsonl"
    temp_output_path = f"/tmp/temp_output_{os.getpid()}_{filename}/"
    
    output_docs = []
    removed_docs_analysis = {}
    stats = {}
    
    try:
        # 파이프라인 입력을 위해 임시 jsonl 파일 작성
        with open(temp_input_path, 'w', encoding='utf-8') as f:
            for doc in all_docs:
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')

        # 파이프라인 생성 및 실행
        pipeline = Pipeline(name=f"filter_{filename}")
        pipeline.add_stage(JsonlReader(file_paths=temp_input_path))
        # QualityFilterStage는 'text' 필드를 자동으로 감지하여 처리
        pipeline.add_stage(QualityFilterStage(filter_config=config))
        pipeline.add_stage(SyntaxAnalysisStage(text_field='text'))
        pipeline.add_stage(JsonlWriter(path=temp_output_path))
        
        executor = XennaExecutor()
        pipeline.run(executor)
        
        # 임시 출력 파일 읽기
        tracked_output_ids = set()
        output_files = glob.glob(os.path.join(temp_output_path, "*.jsonl"))
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        doc = json.loads(line)
                        output_docs.append(doc)
                        if '_tracking_id' in doc:
                            tracked_output_ids.add(doc['_tracking_id'])
                    except json.JSONDecodeError:
                        continue
        
        # 제거된 문서 분석
        removed_docs_analysis = analyze_removed_txt_documents(
            all_docs, all_input_ids, tracked_output_ids, config
        )

        total_output = len(output_docs)
        total_removed = len(all_input_ids) - total_output
        
        logger.info(f"{filename}: 출력={total_output}, 제거={total_removed}")

        stats = {
            "filename": filename,
            "total_input": total_input,
            "total_output": total_output,
            "total_removed": total_removed,
            "breakdown": {k: len(v) for k, v in removed_docs_analysis.items() if v},
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
    
    return removed_docs_analysis, stats, output_docs


def main():
    ray_client = None
    try:
        # Ray 클러스터 초기화 (CPU 사용량 제한 포함)
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
        total_cpus = cpu_count()
        num_cpus = max(1, int(total_cpus * 0.75))
        
        logger.info(f"Ray 초기화 중: 전체 CPU={total_cpus}, 사용할 CPU={num_cpus}, GPU={num_gpus}")
        
        ray_client = RayClient(num_cpus=num_cpus, num_gpus=num_gpus)
        ray_client.start()

        # 출력 디렉토리 생성
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(REMOVED_LOGS_PATH, exist_ok=True)

        # 현재 설정 출력
        print_filter_config(FILTER_CONFIG, PIPELINE_VERSION)

        logger.info("병렬 TXT 파일 처리 시작")
        logger.info(f"입력 디렉토리: {INPUT_DIR}")
        logger.info(f"출력 파일: {OUTPUT_FILENAME}")
        logger.info(f"로그 경로: {REMOVED_LOGS_PATH}")
        print("-" * 70)

        # 입력 파일 목록
        input_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.txt")))
        
        if not input_files:
            logger.error(f"{INPUT_DIR}에 .txt 파일이 없습니다.")
            return
        
        logger.info(f"발견된 파일: {len(input_files)}개")

        # Ray 원격 함수 정의
        @ray.remote(max_retries=1)
        def process_file_remote(input_file):
            return process_txt_file(input_file, FILTER_CONFIG)

        # 병렬 처리 실행
        futures = [process_file_remote.remote(f) for f in input_files]
        
        all_stats = []
        all_good_docs = []
        global_removed_docs = defaultdict(list)
        
        with tqdm(total=len(futures), desc="파일 처리") as pbar:
            for future in futures:
                try:
                    removed_docs, stats, good_docs = ray.get(future)
                    
                    if "error" in stats:
                        logger.error(f"파일 처리 오류 ({stats['filename']}): {stats['error']}")
                    else:
                        all_stats.append(stats)
                        all_good_docs.extend(good_docs)
                        for stage_name, docs in removed_docs.items():
                            global_removed_docs[stage_name].extend(docs)
                except ray.exceptions.RayTaskError as e:
                    logger.error(f"Ray 태스크 오류: {e}")
                except Exception as e:
                    logger.error(f"결과 처리 중 오류: {e}")
                
                pbar.update(1)
        
        # 최종 결과 .txt 파일로 저장
        logger.info(f"필터링된 텍스트를 {OUTPUT_FILENAME} 파일로 저장 중...")
        # 추적 ID를 기준으로 정렬하여 원본 파일 순서를 최대한 유지
        all_good_docs.sort(key=lambda d: d.get('_tracking_id', ''))
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            for doc in all_good_docs:
                # QualityFilterStage를 통과하면 'text' 필드가 'processed_text'로 바뀔 수 있음
                text_to_write = doc.get('processed_text', doc.get('text', ''))
                if text_to_write:
                    f.write(text_to_write + '\n')
        logger.info("저장 완료.")

        # 처리 결과 요약 출력
        print("\n" + "=" * 70)
        print("개별 파일 처리 결과 요약")
        print("=" * 70)
        total_input_docs, total_output_docs = 0, 0
        for stats in sorted(all_stats, key=lambda x: x.get('filename', '')):
            print(f"\n파일: {stats['filename']}")
            print(f"  입력: {stats['total_input']}, 출력: {stats['total_output']}, 제거: {stats['total_removed']}")
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
        print(f"총 입력 라인: {total_input_docs}개")
        print(f"총 출력 라인: {total_output_docs}개")
        print(f"총 제거 라인: {total_input_docs - total_output_docs}개")
        if total_input_docs > 0:
            print(f"보존율: {total_output_docs/total_input_docs*100:.2f}%")
        
        # 제거된 문서 로그 저장
        logger.info("제거된 문서 로그 저장 중...")
        save_global_logs(REMOVED_LOGS_PATH, global_removed_docs)
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
            ray_client.stop()
            logger.info("Ray 종료 완료")


if __name__ == "__main__":
    main()
