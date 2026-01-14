from nemo_curator.core.client import RayClient
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.quality.filter_stage import MultiLingualQualityFilterStage
from nemo_curator.stages.text.preprocessing.korean_preprocessing import KoreanPersonalFilter
from nemo_curator.stages.text.preprocessing.english_preprocessing import EnglishPersonalFilter
from nemo_curator.stages.text.preprocessing.japanese_preprocessing import JapanesePersonalFilter
from nemo_curator.stages.text.preprocessing.chinese_preprocessing import ChinesePersonalFilter
from nemo_curator.stages.text.preprocessing.other_preprocessing import OtherPersonalFilter
from nemo_curator.utils.log_utils import (
    print_filter_config,
    save_global_logs,
    save_preprocessing_logs,
    create_summary,
)
from nemo_curator.core.pipeline_helpers import analyze_removed_documents
from nemo_curator.stages.text.quality.utils import preprocess_text

import os
os.environ['CUDA_VISIBLE_DEVICES'] = ""

import json
import glob
from collections import defaultdict
import shutil
import traceback
import torch
import ray
from multiprocessing import cpu_count
from tqdm import tqdm

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

# 언어 코드 매핑 (데이터셋의 언어 이름과 내부 코드 일치)
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
OUTPUT_PATH = "filtered_data_v3.2/"
REMOVED_LOGS_PATH = "filtered_data_v3.2_log/"
PIPELINE_VERSION = "v3.2"

def process_file_with_tracking(input_file, output_dir, config):
    """개별 파일을 처리하고 제거된 문서 추적 (다국어 지원)"""
    
    filename = os.path.basename(input_file)
    print(f"\n처리 중: {filename}")
    
    all_input_ids = set()
    tracked_output_ids = set()
    
    # 입력 파일 읽기
    all_docs = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line)
                    doc['_line_num'] = line_num
                    doc['_source_file'] = filename
                    doc['_tracking_id'] = f"{filename}::{line_num}"
                    all_docs.append(doc)
                    all_input_ids.add(doc['_tracking_id'])
                except json.JSONDecodeError:
                    continue
    except IOError as e:
        print(f"  ✗ 파일 읽기 오류: {e}")
        return {}, {"error": str(e), "filename": filename}, []

    total_input = len(all_docs)
    print(f"  입력: {total_input}개 문서")

    # 모든 문서에 대한 전처리 변경사항을 미리 기록
    local_prep_logs = []
    for doc in all_docs:
        input_lang = LANG_CODE_MAP.get(doc.get('src', '').lower()) if doc.get('src') else None
        output_lang = LANG_CODE_MAP.get(doc.get('tgt', '').lower()) if doc.get('tgt') else None
        
        original_input = doc.get('input', '')
        processed_input = preprocess_text(original_input, input_lang, PREPROCESSORS)
        if original_input != processed_input:
            local_prep_logs.append({
                '_source_file': doc['_source_file'], '_line_num': doc['_line_num'],
                'field': 'input', 'original': original_input, 'modified': processed_input or "[제거됨]"
            })

        original_output = doc.get('output', '')
        processed_output = preprocess_text(original_output, output_lang, PREPROCESSORS)
        if original_output != processed_output:
            local_prep_logs.append({
                '_source_file': doc['_source_file'], '_line_num': doc['_line_num'],
                'field': 'output', 'original': original_output, 'modified': processed_output or "[제거됨]"
            })
    
    # 파이프라인 생성
    pipeline = Pipeline(name=f"filter_{filename}")
    
    # 임시 파일 경로
    temp_input_path = f"/tmp/temp_input_{filename}"
    temp_output_path = f"/tmp/temp_output_{filename}/"
    
    with open(temp_input_path, 'w', encoding='utf-8') as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')

    pipeline.add_stage(JsonlReader(file_paths=temp_input_path))
    pipeline.add_stage(MultiLingualQualityFilterStage(
        filter_config=config,
        lang_code_map=LANG_CODE_MAP,
        preprocessors=PREPROCESSORS
    ))
    pipeline.add_stage(JsonlWriter(path=temp_output_path))
    
    # 파이프라인 실행
    output_docs = []
    stats = {}
    try:
        executor = XennaExecutor()
        pipeline.run(executor)
        
        output_files = glob.glob(os.path.join(temp_output_path, "*.jsonl"))
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    doc = json.loads(line)
                    output_docs.append(doc)
                    if '_tracking_id' in doc:
                        tracked_output_ids.add(doc['_tracking_id'])
        
        # 최종 출력 파일 저장
        os.makedirs(output_dir, exist_ok=True)
        final_output_path = os.path.join(output_dir, filename)
        with open(final_output_path, 'w', encoding='utf-8') as f:
            for doc in output_docs:
                if 'processing_results' in doc:
                    doc['input'] = doc['processing_results'].get('processed_input')
                    doc['output'] = doc['processing_results'].get('processed_output')

                for key in ['_line_num', '_source_file', '_tracking_id', 'processing_results']:
                    doc.pop(key, None)
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 제거된 문서 분석
        removed_docs = analyze_removed_documents(
            all_docs, all_input_ids, tracked_output_ids, LANG_CODE_MAP, PREPROCESSORS, config
        )

        total_output = len(output_docs)
        total_removed = sum(len(v) for v in removed_docs.values())
        print(f"  출력: {total_output}개, 제거: {total_removed}개, 누락: {len(all_input_ids) - total_output - total_removed}개")

        stats = {
            "filename": filename,
            "total_input": total_input,
            "total_output": total_output,
            "total_removed": total_removed,
            "missing": len(all_input_ids) - total_output - total_removed,
            "breakdown": {k: len(v) for k, v in removed_docs.items() if v},
        }
        
    except Exception as e:
        print(f"  ✗ 처리 실패: {e}")
        traceback.print_exc()
        stats = {"error": str(e), "filename": filename}
    
    finally:
        if os.path.exists(temp_input_path):
            os.remove(temp_input_path)
        if os.path.exists(temp_output_path):
            shutil.rmtree(temp_output_path)
            
    return removed_docs, stats, local_prep_logs

def main():
    ray_client = None
    try:
        # Initialize Ray client
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
        num_cpus = cpu_count()
        print(f"Initializing Ray with {num_cpus} CPUs and {num_gpus} GPUs...")
        ray_client = RayClient(num_cpus=num_cpus, num_gpus=num_gpus)
        ray_client.start()

        # 현재 설정 출력
        print_filter_config(FILTER_CONFIG, PIPELINE_VERSION)

        print("\nStarting parallel file processing...")
        print(f"Input: {INPUT_PATH}")
        print(f"Output: {OUTPUT_PATH}")
        print(f"Logs: {REMOVED_LOGS_PATH}")
        print("-" * 70)

        # 입력 파일 목록
        input_files = glob.glob(os.path.join(INPUT_PATH, "*.jsonl"))
        
        if not input_files:
            print(f"✗ {INPUT_PATH}에 jsonl 파일이 없습니다.")
            return
        
        print(f"\n발견된 파일: {len(input_files)}개")

        # Ray 원격 함수 정의
        @ray.remote
        def process_file_remote(input_file):
            return process_file_with_tracking(
                input_file, OUTPUT_PATH, FILTER_CONFIG
            )

        # 병렬 처리 실행
        futures = [process_file_remote.remote(f) for f in sorted(input_files)]
        
        all_stats = []
        global_removed_docs = defaultdict(list)
        global_preprocessing_logs = []
        print(f"\n{len(futures)}개의 파일을 병렬로 처리합니다...")

        with tqdm(total=len(futures), desc="Total Progress") as pbar:
            while futures:
                done, futures = ray.wait(futures)
                for future in done:
                    try:
                        removed_docs, stats, prep_logs = ray.get(future)
                        global_preprocessing_logs.extend(prep_logs)
                        if "error" in stats:
                            print(f"\n✗ 파일 처리 오류 ({stats['filename']}): {stats['error']}")
                        else:
                            all_stats.append(stats)
                            for stage_name, docs in removed_docs.items():
                                global_removed_docs[stage_name].extend(docs)
                    except Exception as e:
                        print(f"\n✗ Ray future 결과 처리 중 심각한 오류 발생: {e}")
                pbar.update(len(done))
        
        # 처리 결과 요약 출력
        print("\n" + "=" * 70)
        print("개별 파일 처리 결과 요약")
        print("=" * 70)
        all_stats.sort(key=lambda x: x.get('filename', ''))
        total_input_docs = 0
        total_output_docs = 0
        for stats in all_stats:
            print(f"\n파일: {stats['filename']}")
            print(f"  - 입력: {stats['total_input']}, 출력: {stats['total_output']}, 제거: {stats['total_removed']}, 누락: {stats['missing']}")
            total_input_docs += stats['total_input']
            total_output_docs += stats['total_output']
            if stats.get('breakdown'):
                print("  - 제거 상세:")
                for stage, count in stats['breakdown'].items():
                    print(f"    - {stage}: {count}개")
        
        print("\n" + "=" * 70)
        print("전체 요약")
        print("=" * 70)
        print(f"총 처리 파일: {len(all_stats)}개")
        print(f"총 입력 문서: {total_input_docs}개")
        print(f"총 출력 문서: {total_output_docs}개")
        
        # 전역 로그 저장 (기준별로)
        save_global_logs(REMOVED_LOGS_PATH, global_removed_docs)
        save_preprocessing_logs(REMOVED_LOGS_PATH, global_preprocessing_logs)
        
        # 전체 요약 생성
        create_summary(REMOVED_LOGS_PATH, global_removed_docs, PIPELINE_VERSION)
        
        print("\n" + "=" * 70)
        print("✓ 모든 파일 처리 완료!")
        print("=" * 70)

    except Exception as e:
        print("\n" + "=" * 70)
        print("✗ 처리 실패")
        print("=" * 70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if ray_client:
            print("\n" + "-" * 70)
            print("Shutting down Ray cluster...")
            ray_client.stop()
            print("Done.")


if __name__ == "__main__":
    main()
