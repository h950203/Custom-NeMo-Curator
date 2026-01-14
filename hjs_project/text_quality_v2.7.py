from nemo_curator.core.client import RayClient
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.modules import Modify, Filter
from nemo_curator.stages.text.filters.korean_personal_filter import KoreanPersonalFilter
from nemo_curator.stages.text.filters.english_personal_filter import EnglishPersonalFilter
from nemo_curator.stages.text.filters.base_personal_filter import BasePersonalFilter
import os
import json
import re
import kss
import glob
from collections import defaultdict
import time
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
}

# 언어별 전처리기
PREPROCESSORS = {
    "ko": KoreanPersonalFilter(),
    "en": EnglishPersonalFilter(),
}
DEFAULT_PREPROCESSOR = BasePersonalFilter()


INPUT_PATH = "data/"
OUTPUT_PATH = "filtered_data_v2.7/"
REMOVED_LOGS_PATH = "filtered_data_v2.7/log/"
PIPELINE_VERSION = "v2.7"

# 전역 변수: 모든 파일의 제거된 문서를 기준별로 모음
GLOBAL_REMOVED_DOCS = defaultdict(list)

def custom_normalize_whitespace(text):
    """공백 정규화"""
    return re.sub(r'\s+', ' ', text).strip()

def print_filter_config(config):
    print("\n" + "=" * 70)
    print(f"MULTI-LINGUAL TEXT FILTERING - {PIPELINE_VERSION}")
    print("=" * 70)
    for lang, lang_config in config.items():
        if lang in ['defaults', 'quality_weights']: continue
        print(f"\n[Language: {lang.upper()}]")
        merged_config = {**config['defaults'], **lang_config}
        print(f"  • 단어 수: {merged_config.get('min_words', 'N/A')} ~ {merged_config.get('max_words', 'N/A')}")
        print(f"  • {lang} 문자 비율: ≥ {merged_config.get('lang_threshold', 'N/A')}")
    print("\n[Common Filters]")
    print(f"  • 반복 라인 비율: ≤ {config['defaults']['max_repeated_line_fraction']}")
    print(f"  • 반복 N-gram (n={config['defaults']['ngram_n']}): ≤ {config['defaults']['max_repeating_ngram_ratio']}")
    print(f"  • 특수기호/단어 비율: ≤ {config['defaults']['max_symbol_to_word_ratio']}")
    print(f"  • URL 비율: ≤ {config['defaults']['max_url_ratio']}")
    print(f"  • 최종 품질 점수: ≥ {config['defaults']['min_quality_score']}")
    print("=" * 70)


def calculate_scores(text, lang, config):
    scores = {}
    lang_config = {**config['defaults'], **config.get(lang, {})}
    
    words = text.split()
    scores['word_count'] = len(words)
    
    total_chars = len(re.sub(r'\s', '', text))
    if total_chars > 0 and 'lang_char_pattern' in lang_config:
        lang_chars = len(re.findall(lang_config['lang_char_pattern'], text))
        scores['lang_ratio'] = lang_chars / total_chars
    else:
        scores['lang_ratio'] = 0

    lines = text.split('\n')
    scores['repeated_lines_uniqueness_ratio'] = len(set(lines)) / len(lines) if lines else 0

    n = lang_config['ngram_n']
    ngrams = [tuple(words[i:i+n]) for i in range(len(words)-n+1)]
    scores['repeating_duplicate_ngram_ratio'] = 1 - (len(set(ngrams)) / len(ngrams)) if ngrams else 0
    
    if words:
        scores['symbol_to_word_ratio'] = len(re.findall(r'[^\w\s가-힣a-zA-Z]', text)) / len(words)
        scores['urls_ratio'] = len(re.findall(r'https?://\S+', text)) / len(words)
    else:
        scores['symbol_to_word_ratio'] = 0
        scores['urls_ratio'] = 0

    weights = config['quality_weights']
    scores['quality_score'] = (
        scores.get('lang_ratio', 0) * weights['lang_ratio'] +
        scores.get('repeated_lines_uniqueness_ratio', 0) * weights['repeated_lines'] +
        (1.0 - scores.get('repeating_duplicate_ngram_ratio', 0)) * weights['repeated_ngrams'] +
        (1.0 - scores.get('symbol_to_word_ratio', 0)) * weights['symbols'] +
        (1.0 - scores.get('urls_ratio', 0)) * weights['urls']
    )
    return scores

def preprocess_text(text, lang, preprocessors):
    """Helper to preprocess a single text string."""
    if not text or not text.strip():
        return ""
    
    # kss for Korean sentence splitting
    if lang == 'ko':
        sentences = kss.split_sentences(text)
    else:
        # Basic sentence splitting for other languages
        sentences = text.split('. ')

    # Apply personal data filter
    preprocessor = preprocessors.get(lang, DEFAULT_PREPROCESSOR)
    processed_sentences = [preprocessor.apply(s) for s in sentences]
    processed_sentences = [s for s in processed_sentences if s]

    if not processed_sentences:
        return ""

    # Join and normalize
    joined_text = " ".join(processed_sentences)
    return custom_normalize_whitespace(joined_text)

def run_filters_on_text(text, lang, config, scores):
    """Check if a processed text passes all filters."""
    lang_config = {**config['defaults'], **config.get(lang, {})}

    if not text or not text.strip():
        return False, "preprocessing_empty", "전처리 후 빈 텍스트"
    if 'min_words' in lang_config and not (lang_config['min_words'] <= scores['word_count'] <= lang_config['max_words']):
        return False, "word_count", f"단어 수: {scores['word_count']}"
    if 'lang_threshold' in lang_config and scores['lang_ratio'] < lang_config['lang_threshold']:
        return False, "language", f"{lang} 문자 비율: {scores['lang_ratio']:.3f}"
    if (1 - scores['repeated_lines_uniqueness_ratio']) > lang_config['max_repeated_line_fraction']:
        return False, "repeated_lines", f"반복 라인 비율: {1 - scores['repeated_lines_uniqueness_ratio']:.3f}"
    if scores['repeating_duplicate_ngram_ratio'] > lang_config['max_repeating_ngram_ratio']:
        return False, "repeated_ngrams", f"반복 N-gram 비율: {scores['repeating_duplicate_ngram_ratio']:.3f}"
    if scores['symbol_to_word_ratio'] > lang_config['max_symbol_to_word_ratio']:
        return False, "symbols", f"특수기호 비율: {scores['symbol_to_word_ratio']:.3f}"
    if scores['urls_ratio'] > lang_config['max_url_ratio']:
        return False, "urls", f"URL 비율: {scores['urls_ratio']:.3f}"
    if scores['quality_score'] < lang_config['min_quality_score']:
        return False, "quality_score", f"품질 점수: {scores['quality_score']:.3f}"
        
    return True, "passed", ""


def process_file_with_tracking(input_file, output_dir, config):
    """개별 파일을 처리하고 제거된 문서 추적 (다국어 지원)"""
    
    filename = os.path.basename(input_file)
    print(f"\n처리 중: {filename}")
    
    removed_docs = defaultdict(list)
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
        return {}, {"error": str(e), "filename": filename}
    
    total_input = len(all_docs)
    print(f"  입력: {total_input}개 문서")
    
    # 파이프라인 생성
    pipeline = Pipeline(name=f"filter_{filename}")
    
    # 임시 파일 경로
    temp_input_path = f"/tmp/temp_input_{filename}"
    temp_output_path = f"/tmp/temp_output_{filename}/"
    
    with open(temp_input_path, 'w', encoding='utf-8') as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')
    
    # 파이프라인 스테이지 정의
    def process_and_score_fields(input, src, output, tgt):
        try:
            input_lang = LANG_CODE_MAP.get(src.lower()) if src else None
            output_lang = LANG_CODE_MAP.get(tgt.lower()) if tgt else None

            processed_input = preprocess_text(input, input_lang, PREPROCESSORS)
            processed_output = preprocess_text(output, output_lang, PREPROCESSORS)
            
            input_scores = calculate_scores(processed_input, input_lang, config)
            output_scores = calculate_scores(processed_output, output_lang, config)
            
            return {
                'processed_input': processed_input, 'processed_output': processed_output,
                'input_scores': input_scores, 'output_scores': output_scores,
                'input_lang': input_lang, 'output_lang': output_lang,
            }
        except Exception as e:
            return {'error': str(e)}

    def final_filter_doc(processing_results):
        if not processing_results or 'error' in processing_results:
            return False
        
        input_passed, _, _ = run_filters_on_text(
            processing_results['processed_input'], processing_results['input_lang'], config, processing_results['input_scores']
        )
        if not input_passed:
            return False
        
        output_passed, _, _ = run_filters_on_text(
            processing_results['processed_output'], processing_results['output_lang'], config, processing_results['output_scores']
        )
        return output_passed

    pipeline.add_stage(JsonlReader(file_paths=temp_input_path))
    pipeline.add_stage(Modify(
        modifier_fn=process_and_score_fields,
        input_fields=[['input', 'src', 'output', 'tgt']],
        output_fields='processing_results'
    ))
    pipeline.add_stage(Filter(filter_fn=final_filter_doc, filter_field='processing_results'))
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
                # 불필요한 필드 제거
                for key in ['_line_num', '_source_file', '_tracking_id', 'processing_results']:
                    doc.pop(key, None)
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 제거된 문서 분석
        removed_ids = all_input_ids - tracked_output_ids
        for doc in all_docs:
            if doc['_tracking_id'] in removed_ids:
                processing_results = process_and_score_fields(
                    doc.get('input', ''), doc.get('src', ''),
                    doc.get('output', ''), doc.get('tgt', '')
                )

                if 'error' in processing_results:
                    removed_docs['preprocessing_error'].append({**doc, 'reason': processing_results['error']})
                    continue

                log_entry = {**doc, **processing_results}

                input_passed, input_key, input_reason = run_filters_on_text(
                    log_entry['processed_input'], log_entry['input_lang'], config, log_entry['input_scores']
                )
                if not input_passed:
                    reason_key = f"input_{input_key}"
                    removed_docs[reason_key].append({**log_entry, 'reason': input_reason})
                    continue

                output_passed, output_key, output_reason = run_filters_on_text(
                    log_entry['processed_output'], log_entry['output_lang'], config, log_entry['output_scores']
                )
                if not output_passed:
                    reason_key = f"output_{output_key}"
                    removed_docs[reason_key].append({**log_entry, 'reason': output_reason})
                    continue
                
                removed_docs['unknown_removal'].append({**log_entry, 'reason': '알 수 없는 이유로 파이프라인에서 누락됨'})

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
        import traceback
        traceback.print_exc()
        stats = {"error": str(e), "filename": filename}
    
    finally:
        if os.path.exists(temp_input_path):
            os.remove(temp_input_path)
        if os.path.exists(temp_output_path):
            import shutil
            shutil.rmtree(temp_output_path)
            
    return removed_docs, stats

def save_global_logs(logs_dir):
    """전역 변수에 모인 제거 문서들을 기준별로 저장"""
    os.makedirs(logs_dir, exist_ok=True)
    print(f"\n로그 파일 생성 중...")
    
    for stage_name, docs in GLOBAL_REMOVED_DOCS.items():
        if not docs: continue
        log_file = os.path.join(logs_dir, f"{stage_name}.txt")
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"필터링 기준: {stage_name}\n제거된 문서 수: {len(docs)}\n" + "=" * 80 + "\n\n")
            for doc_info in docs:
                f.write(f"\n{'='*80}\n")
                f.write(f"파일: {doc_info['_source_file']}\n")
                f.write(f"라인: {doc_info['_line_num']}\n")
                f.write(f"제거 이유: {doc_info['reason']}\n")
                
                if doc_info.get('input_scores'):
                    f.write("\n[Input Scores]\n" + "\n".join([f"  - {k}: {v}" for k, v in doc_info['input_scores'].items()]) + "\n")
                if doc_info.get('output_scores'):
                    f.write("\n[Output Scores]\n" + "\n".join([f"  - {k}: {v}" for k, v in doc_info['output_scores'].items()]) + "\n")

                f.write(f"\n[Original Input]\n{doc_info.get('input', '')}\n")
                if 'processed_input' in doc_info:
                    f.write(f"\n[Processed Input]\n{doc_info.get('processed_input', '')}\n")
                f.write(f"\n[Original Output]\n{doc_info.get('output', '')}\n")
                if 'processed_output' in doc_info:
                    f.write(f"\n[Processed Output]\n{doc_info.get('processed_output', '')}\n")
                f.write("\n")
        print(f"  ✓ {stage_name}.txt ({len(docs)}개 문서)")


def create_summary(logs_dir):
    """전체 요약 파일 생성"""
    summary = {stage: len(docs) for stage, docs in GLOBAL_REMOVED_DOCS.items()}
    file_stats = defaultdict(lambda: defaultdict(int))
    for stage, docs in GLOBAL_REMOVED_DOCS.items():
        for doc in docs:
            file_stats[doc['_source_file']][stage] += 1
    
    summary_file = os.path.join(logs_dir, "summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"필터링 요약 - {PIPELINE_VERSION}\n" + "=" * 80 + "\n\n")
        total_removed = sum(summary.values())
        f.write(f"총 제거된 문서 수: {total_removed}\n\n")
        f.write("단계별 제거 현황:\n" + "-" * 80 + "\n")
        for stage_name in sorted(summary.keys()):
            count = summary.get(stage_name, 0)
            f.write(f"{stage_name:40s}: {count:6d} 개\n")
        if file_stats:
            f.write("\n\n파일별 제거 현황:\n" + "-" * 80 + "\n")
            for file_name in sorted(file_stats.keys()):
                f.write(f"\n{file_name}:\n")
                for stage, count in sorted(file_stats[file_name].items()):
                    f.write(f"  {stage:38s}: {count:6d} 개\n")
        f.write("-" * 80 + "\n")
    print(f"\n✓ 요약 파일 생성: {summary_file}")


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
        print_filter_config(FILTER_CONFIG)

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
        def process_file_remote(input_file, output_dir, config):
            return process_file_with_tracking(input_file, output_dir, config)

        # 병렬 처리 실행
        futures = [process_file_remote.remote(f, OUTPUT_PATH, FILTER_CONFIG) for f in sorted(input_files)]
        
        all_stats = []
        print(f"\n{len(futures)}개의 파일을 병렬로 처리합니다...")

        with tqdm(total=len(futures), desc="Total Progress") as pbar:
            while futures:
                done, futures = ray.wait(futures)
                for future in done:
                    try:
                        removed_docs, stats = ray.get(future)
                        if "error" in stats:
                            print(f"\n✗ 파일 처리 오류 ({stats['filename']}): {stats['error']}")
                        else:
                            all_stats.append(stats)
                            for stage_name, docs in removed_docs.items():
                                GLOBAL_REMOVED_DOCS[stage_name].extend(docs)
                    except Exception as e:
                        print(f"\n✗ Ray future 결과 처리 중 심각한 오류 발생: {e}")
                pbar.update(len(done))
        
        # 처리 결과 요약 출력
        print("\n" + "=" * 70)
        print("개별 파일 처리 결과 요약")
        print("=" * 70)
        all_stats.sort(key=lambda x: x['filename'])
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
        save_global_logs(REMOVED_LOGS_PATH)
        
        # 전체 요약 생성
        create_summary(REMOVED_LOGS_PATH)
        
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
