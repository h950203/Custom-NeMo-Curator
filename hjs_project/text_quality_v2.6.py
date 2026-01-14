from nemo_curator.core.client import RayClient
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.modules import ScoreFilter, Modify, Filter
from nemo_curator.stages.text.filters import (
    WordCountFilter,
    HistogramFilter,
    MeanWordLengthFilter,
    PersonalFilter,
)
from nemo_curator.stages.text.filters.heuristic_filter import (
    RepeatedLinesFilter,
    SymbolsToWordsFilter,
    UrlsFilter,
    RepeatingDuplicateNGramsFilter,
)
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


start_time = time.time()
# ============================================================================
# 필터링 기준 설정
# ============================================================================
FILTER_CONFIG = {
    'min_words': 3,
    'max_words': 100000,
    'lang_threshold': 0.5,
    'max_repeated_line_fraction': 0.7,
    'ngram_n': 5,
    'max_repeating_ngram_ratio': 0.2,
    'min_mean_word_length': 2,
    'max_mean_word_length': 15,
    'max_symbol_to_word_ratio': 0.8,
    'max_url_ratio': 0.01,
    'min_quality_score': 0.6,
    'quality_weights': {
        'repeated_lines': 0.25,
        'repeated_ngrams': 0.25,
        'symbols': 0.25,
        'urls': 0.25
    }
}

INPUT_PATH = "data/"
OUTPUT_PATH = "filtered_data_v2.6/"
REMOVED_LOGS_PATH = "filtered_data_v2.6/log/"
PIPELINE_VERSION = "v2.6"

# 전역 변수: 모든 파일의 제거된 문서를 기준별로 모음
GLOBAL_REMOVED_DOCS = defaultdict(list)


def custom_normalize_whitespace(text):
    """공백 정규화"""
    return re.sub(r'\s+', ' ', text).strip()


def print_filter_config(config):
    """현재 필터 설정 출력"""
    print("\n" + "=" * 70)
    print(f"KOREAN TEXT FILTERING - {PIPELINE_VERSION}")
    print("=" * 70)
    print("\n현재 필터링 기준:")
    print(f"  • 단어 수: {config['min_words']} ~ {config['max_words']}")
    print(f"  • 한국어 비율: ≥ {config['lang_threshold']}")
    print(f"  • 반복 라인 비율: ≤ {config['max_repeated_line_fraction']}")
    print(f"  • 반복 N-gram (n={config['ngram_n']}): ≤ {config['max_repeating_ngram_ratio']}")
    print(f"  • 평균 단어 길이: {config['min_mean_word_length']} ~ {config['max_mean_word_length']}")
    print(f"  • 특수기호/단어 비율: ≤ {config['max_symbol_to_word_ratio']}")
    print(f"  • URL 비율: ≤ {config['max_url_ratio']}")
    print(f"  • 최종 품질 점수: ≥ {config['min_quality_score']}")
    print("=" * 70)


def calculate_all_scores(text, config):
    """텍스트의 모든 점수 계산"""
    scores = {}
    
    # 단어 수
    words = text.split()
    scores['word_count'] = len(words)
    
    # 한국어 비율
    korean_chars = len(re.findall(r'[가-힣]', text))
    total_chars = len(re.sub(r'\s', '', text))
    scores['korean_ratio'] = korean_chars / total_chars if total_chars > 0 else 0
    
    # 반복 라인 비율
    lines = text.split('\n')
    unique_lines = set(lines)
    scores['repeated_lines_uniqueness_ratio'] = len(unique_lines) / len(lines) if lines else 0
    
    # 반복 N-gram 비율
    n = config['ngram_n']
    ngrams = [tuple(words[i:i+n]) for i in range(len(words)-n+1)]
    unique_ngrams = set(ngrams)
    scores['repeating_duplicate_ngram_ratio'] = 1 - (len(unique_ngrams) / len(ngrams)) if ngrams else 0
    
    # 평균 단어 길이
    scores['mean_word_length'] = sum(len(w) for w in words) / len(words) if words else 0
    
    # 특수기호/단어 비율
    symbols = len(re.findall(r'[^\w\s가-힣]', text))
    scores['symbol_to_word_ratio'] = symbols / len(words) if words else 0
    
    # URL 비율
    urls = len(re.findall(r'https?://\S+', text))
    scores['urls_ratio'] = urls / len(words) if words else 0
    
    # 품질 점수 계산
    weights = config['quality_weights']
    score_repeated_lines = scores['repeated_lines_uniqueness_ratio']
    score_ngrams = 1.0 - scores['repeating_duplicate_ngram_ratio']
    score_symbols = 1.0 - scores['symbol_to_word_ratio']
    score_urls = 1.0 - scores['urls_ratio']
    
    scores['quality_score'] = (
        score_repeated_lines * weights['repeated_lines'] +
        score_ngrams * weights['repeated_ngrams'] +
        score_symbols * weights['symbols'] +
        score_urls * weights['urls']
    )
    
    return scores


def process_file_with_tracking(input_file, output_dir, config):
    """개별 파일을 처리하고 제거된 문서 추적 (전역 변수에 저장) - 누락 추적 개선"""
    
    filename = os.path.basename(input_file)
    print(f"\n처리 중: {filename}")
    
    # 제거된 문서 추적 (로컬)
    removed_docs = defaultdict(list)
    
    # ========== 추가: 모든 문서 ID 추적 ==========
    all_input_ids = set()
    tracked_output_ids = set()
    tracked_removed_ids = set()
    
    # 입력 파일 읽기
    all_docs = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line)
                    doc['_line_num'] = line_num
                    doc['_source_file'] = filename
                    doc['_tracking_id'] = f"{filename}::{line_num}"  # 고유 ID 추가
                    all_docs.append(doc)
                    all_input_ids.add(doc['_tracking_id'])
                except json.JSONDecodeError:
                    continue
    except IOError as e:
        print(f"  ✗ 파일 읽기 오류: {e}")
        return {}, {"error": str(e), "filename": filename}
    
    total_input = len(all_docs)
    print(f"  입력: {total_input}개 문서")
    
    # 파이프라인 생성 (기존 코드 동일)
    pipeline = Pipeline(
        name=f"filter_{filename}",
        description=f"Processing {filename}"
    )
    
    temp_input = f"/tmp/temp_input_{filename}"
    temp_output = f"/tmp/temp_output_{filename}/"
    
    # 임시 파일에 쓰기
    with open(temp_input, 'w', encoding='utf-8') as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')
    
    reader = JsonlReader(file_paths=temp_input)
    pipeline.add_stage(reader)
    
    # === STAGE 1: PREPROCESSING (기존과 동일) ===
    def split_into_sentences(text):
        return kss.split_sentences(text)
    
    pipeline.add_stage(Modify(
        modifier_fn=split_into_sentences,
        input_fields="text",
        output_fields="sentences"
    ))
    
    personal_preprocessor = PersonalFilter()
    
    def process_sentences(sentences):
        processed = [personal_preprocessor.apply(s) for s in sentences]
        return [s for s in processed if s]
    
    pipeline.add_stage(Modify(
        modifier_fn=process_sentences,
        input_fields="sentences",
        output_fields="sentences"
    ))
    
    def join_sentences(sentences):
        return " ".join(sentences) if sentences else ""
    
    pipeline.add_stage(Modify(
        modifier_fn=join_sentences,
        input_fields="sentences",
        output_fields="text"
    ))
    
    pipeline.add_stage(Modify(
        modifier_fn=custom_normalize_whitespace,
        input_fields="text",
        output_fields="text"
    ))
    
    # === STAGE 2~4: 필터링 (기존과 동일) ===
    filters_config = [
        ("word_count", WordCountFilter(min_words=config['min_words'], 
                                      max_words=config['max_words'], lang='ko'), "word_count"),
        ("language", HistogramFilter(lang='ko', threshold=config['lang_threshold']), "lang_score"),
        ("repeated_lines", RepeatedLinesFilter(max_repeated_line_fraction=config['max_repeated_line_fraction']), 
         "repeated_lines_uniqueness_ratio"),
        ("repeated_ngrams", RepeatingDuplicateNGramsFilter(n=config['ngram_n'], 
                                                          max_repeating_duplicate_ngram_ratio=config['max_repeating_ngram_ratio'], 
                                                          lang='ko'), "repeating_duplicate_ngram_ratio"),
        ("mean_word_length", MeanWordLengthFilter(min_mean_word_length=config['min_mean_word_length'], 
                                                 max_mean_word_length=config['max_mean_word_length'], lang='ko'), 
         "mean_word_length"),
        ("symbols", SymbolsToWordsFilter(max_symbol_to_word_ratio=config['max_symbol_to_word_ratio'], lang='ko'), 
         "symbol_to_word_ratio"),
        ("urls", UrlsFilter(max_url_to_text_ratio=config['max_url_ratio']), "urls_ratio"),
    ]
    
    for filter_name, filter_obj, score_field in filters_config:
        score_filter = ScoreFilter(
            filter_obj=filter_obj,
            text_field="text",
            score_field=score_field
        )
        pipeline.add_stage(score_filter)
    
    weights = config['quality_weights']
    
    def calculate_quality_score_normalized(repeated_lines_uniqueness_ratio, repeating_duplicate_ngram_ratio, 
                                          symbol_to_word_ratio, urls_ratio):
        score_repeated_lines = repeated_lines_uniqueness_ratio
        score_ngrams = 1.0 - repeating_duplicate_ngram_ratio
        score_symbols = 1.0 - symbol_to_word_ratio
        score_urls = 1.0 - urls_ratio
        
        quality_score = (
            score_repeated_lines * weights['repeated_lines'] +
            score_ngrams * weights['repeated_ngrams'] +
            score_symbols * weights['symbols'] +
            score_urls * weights['urls']
        )
        
        return quality_score
    
    quality_score_stage = Modify(
        modifier_fn=calculate_quality_score_normalized,
        input_fields=[["repeated_lines_uniqueness_ratio", "repeating_duplicate_ngram_ratio", 
                      "symbol_to_word_ratio", "urls_ratio"]],
        output_fields="quality_score"
    )
    pipeline.add_stage(quality_score_stage)
    
    final_quality_score_threshold_fn = lambda score: score >= config['min_quality_score']
    
    final_filter_stage = Filter(
        filter_fn=final_quality_score_threshold_fn,
        filter_field="quality_score"
    )
    pipeline.add_stage(final_filter_stage)
    
    writer = JsonlWriter(path=temp_output)
    pipeline.add_stage(writer)
    
    # Execute pipeline
    executor = XennaExecutor()
    
    output_docs = []
    try:
        pipeline.run(executor)
        
        # 출력된 문서 읽기
        output_files = glob.glob(os.path.join(temp_output, "*.jsonl"))
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        doc = json.loads(line)
                        output_docs.append(doc)
                        # 출력된 문서 ID 추적
                        if '_tracking_id' in doc:
                            tracked_output_ids.add(doc['_tracking_id'])
                    except json.JSONDecodeError:
                        continue
        
        # 최종 출력 파일 생성
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for doc in output_docs:
                doc.pop('_line_num', None)
                doc.pop('_source_file', None)
                doc.pop('_tracking_id', None)
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # ========== 개선: 제거된 문서 분석 ==========
        personal_preprocessor = PersonalFilter()
        
        for original_doc in all_docs:
            tracking_id = original_doc['_tracking_id']
            
            if tracking_id in tracked_output_ids:
                continue  # 살아남은 문서
            
            # 이미 추적된 문서는 스킵 (중복 방지)
            if tracking_id in tracked_removed_ids:
                continue
            
            line_num = original_doc['_line_num']
            source_file = original_doc['_source_file']
            text = original_doc.get('text', '')
            
            # ===== 전처리 시뮬레이션 =====
            try:
                # 1. 원본 빈 텍스트
                if not text or not text.strip():
                    removed_docs['preprocessing_empty'].append({
                        'source_file': source_file,
                        'line_num': line_num,
                        'text': text,
                        'reason': '원본이 빈 텍스트',
                        'scores': {}
                    })
                    tracked_removed_ids.add(tracking_id)
                    continue
                
                # 2. 문장 분리
                sentences = kss.split_sentences(text)
                if not sentences:
                    removed_docs['preprocessing_empty'].append({
                        'source_file': source_file,
                        'line_num': line_num,
                        'text': text[:500],
                        'reason': 'kss 문장 분리 결과 없음',
                        'scores': {}
                    })
                    tracked_removed_ids.add(tracking_id)
                    continue
                
                # 3. PersonalFilter 처리
                processed_sentences = [personal_preprocessor.apply(s) for s in sentences]
                processed_sentences = [s for s in processed_sentences if s]
                
                if not processed_sentences:
                    removed_docs['preprocessing_empty'].append({
                        'source_file': source_file,
                        'line_num': line_num,
                        'text': text[:500],
                        'reason': 'PersonalFilter 처리 후 빈 문장',
                        'scores': {}
                    })
                    tracked_removed_ids.add(tracking_id)
                    continue
                
                # 4. 문장 결합 및 정규화
                processed_text = " ".join(processed_sentences)
                processed_text = custom_normalize_whitespace(processed_text)
                
                if not processed_text or not processed_text.strip():
                    removed_docs['preprocessing_empty'].append({
                        'source_file': source_file,
                        'line_num': line_num,
                        'text': text[:500],
                        'reason': '공백 정규화 후 빈 텍스트',
                        'scores': {}
                    })
                    tracked_removed_ids.add(tracking_id)
                    continue
                
            except Exception as e:
                removed_docs['preprocessing_error'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'reason': f'전처리 오류: {str(e)}',
                    'scores': {}
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            # ===== 필터 점수 계산 =====
            scores = calculate_all_scores(processed_text, config)
            
            # 각 필터 체크 (기존 로직과 동일하지만 tracked_removed_ids 추가)
            if scores['word_count'] < config['min_words'] or scores['word_count'] > config['max_words']:
                removed_docs['word_count'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'단어 수: {scores["word_count"]} (기준: {config["min_words"]}~{config["max_words"]})',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['korean_ratio'] < config['lang_threshold']:
                removed_docs['language'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'한국어 비율: {scores["korean_ratio"]:.3f} (기준: ≥{config["lang_threshold"]})',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['repeated_lines_uniqueness_ratio'] < (1 - config['max_repeated_line_fraction']):
                removed_docs['repeated_lines'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'반복 라인 비율: {1 - scores["repeated_lines_uniqueness_ratio"]:.3f}',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['repeating_duplicate_ngram_ratio'] > config['max_repeating_ngram_ratio']:
                removed_docs['repeated_ngrams'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'반복 N-gram 비율: {scores["repeating_duplicate_ngram_ratio"]:.3f}',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['mean_word_length'] < config['min_mean_word_length'] or scores['mean_word_length'] > config['max_mean_word_length']:
                removed_docs['mean_word_length'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'평균 단어 길이: {scores["mean_word_length"]:.2f}',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['symbol_to_word_ratio'] > config['max_symbol_to_word_ratio']:
                removed_docs['symbols'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'특수기호 비율: {scores["symbol_to_word_ratio"]:.3f}',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['urls_ratio'] > config['max_url_ratio']:
                removed_docs['urls'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'URL 비율: {scores["urls_ratio"]:.3f}',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            if scores['quality_score'] < config['min_quality_score']:
                removed_docs['quality_score'].append({
                    'source_file': source_file,
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'품질 점수: {scores["quality_score"]:.3f}',
                    'scores': scores
                })
                tracked_removed_ids.add(tracking_id)
                continue
            
            # ========== 추가: 누락 문서 추적 ==========
            # 모든 필터를 통과했는데도 출력되지 않은 경우
            removed_docs['unknown_removal'].append({
                'source_file': source_file,
                'line_num': line_num,
                'text': text[:500],
                'processed_text': processed_text[:500],
                'reason': '모든 필터 통과했으나 파이프라인에서 누락됨',
                'scores': scores
            })
            tracked_removed_ids.add(tracking_id)
        
        # ========== 추가: 누락 검증 ==========
        missing_ids = all_input_ids - tracked_output_ids - tracked_removed_ids
        
        if missing_ids:
            print(f"  ⚠️  경고: {len(missing_ids)}개 문서가 추적되지 않음!")
            for missing_id in list(missing_ids)[:5]:  # 처음 5개만 표시
                print(f"     - {missing_id}")
        
        total_removed = sum(len(docs) for docs in removed_docs.values())
        total_output = len(output_docs)
        
        # ========== 개선: 상세 통계 출력 ==========
        print(f"  출력: {total_output}개")
        print(f"  제거: {total_removed}개")
        print(f"  누락: {len(missing_ids)}개")
        print(f"  합계 검증: {total_output + total_removed + len(missing_ids)} (입력: {total_input})")
        
        if removed_docs:
            print(f"  제거 상세:")
            for stage_name, docs in removed_docs.items():
                if docs:
                    print(f"    - {stage_name}: {len(docs)}개")

        stats = {
            "filename": filename,
            "total_input": total_input,
            "total_output": total_output,
            "total_removed": total_removed,
            "missing": len(missing_ids),
            "breakdown": {k: len(v) for k, v in removed_docs.items() if v},
        }
        
    except Exception as e:
        print(f"  ✗ 처리 실패: {e}")
        import traceback
        traceback.print_exc()
        stats = {"error": str(e), "filename": filename}
    
    finally:
        if os.path.exists(temp_input):
            os.remove(temp_input)
        if os.path.exists(temp_output):
            import shutil
            shutil.rmtree(temp_output)
            
    return removed_docs, stats


def save_global_logs(logs_dir):
    """전역 변수에 모인 제거 문서들을 기준별로 저장"""
    
    os.makedirs(logs_dir, exist_ok=True)
    
    print(f"\n로그 파일 생성 중...")
    
    for stage_name, docs in GLOBAL_REMOVED_DOCS.items():
        if not docs:
            continue
        
        # 기준별로 하나의 파일 생성 (파일명 없이)
        log_file = os.path.join(logs_dir, f"{stage_name}.txt")
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"=" * 80 + "\n")
            f.write(f"필터링 기준: {stage_name}\n")
            f.write(f"제거된 문서 수: {len(docs)}\n")
            f.write(f"=" * 80 + "\n\n")
            
            for doc_info in docs:
                f.write(f"\n{'='*80}\n")
                f.write(f"파일: {doc_info['source_file']}\n")
                f.write(f"라인 번호: {doc_info['line_num']}\n")
                f.write(f"제거 이유: {doc_info['reason']}\n")
                
                # 점수 정보 추가
                if doc_info.get('scores'):
                    f.write(f"\n상세 점수:\n")
                    scores = doc_info['scores']
                    f.write(f"  - 단어 수: {scores.get('word_count', 'N/A')}\n")
                    f.write(f"  - 한국어 비율: {scores.get('korean_ratio', 0):.3f}\n")
                    f.write(f"  - 반복 라인 고유성: {scores.get('repeated_lines_uniqueness_ratio', 0):.3f}\n")
                    f.write(f"  - 반복 N-gram 비율: {scores.get('repeating_duplicate_ngram_ratio', 0):.3f}\n")
                    f.write(f"  - 평균 단어 길이: {scores.get('mean_word_length', 0):.2f}\n")
                    f.write(f"  - 특수기호/단어 비율: {scores.get('symbol_to_word_ratio', 0):.3f}\n")
                    f.write(f"  - URL 비율: {scores.get('urls_ratio', 0):.3f}\n")
                    f.write(f"  - 최종 품질 점수: {scores.get('quality_score', 0):.3f}\n")
                
                f.write(f"\n원본 텍스트:\n{doc_info['text']}\n")
                if 'processed_text' in doc_info:
                    f.write(f"\n처리된 텍스트:\n{doc_info['processed_text']}\n")
                f.write("\n")
        
        print(f"  ✓ {stage_name}.txt ({len(docs)}개 문서)")


def create_summary(logs_dir):
    """전체 요약 파일 생성"""
    
    summary = {}
    file_stats = defaultdict(lambda: defaultdict(int))
    
    # 전역 변수에서 통계 수집
    for stage_name, docs in GLOBAL_REMOVED_DOCS.items():
        summary[stage_name] = len(docs)
        
        # 파일별 통계
        for doc in docs:
            file_stats[doc['source_file']][stage_name] += 1
    
    summary_file = os.path.join(logs_dir, "summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"=" * 80 + "\n")
        f.write(f"필터링 요약 - {PIPELINE_VERSION}\n")
        f.write(f"=" * 80 + "\n\n")
        
        total_removed = sum(summary.values())
        f.write(f"총 제거된 문서 수: {total_removed}\n\n")
        
        f.write("단계별 제거 현황:\n")
        f.write("-" * 80 + "\n")
        
        # 모든 단계를 정의된 순서대로 표시
        all_stages = ['preprocessing_empty', 'preprocessing_error', 'word_count', 
                     'language', 'repeated_lines', 'repeated_ngrams', 
                     'mean_word_length', 'symbols', 'urls', 'quality_score', 'unknown_removal']
        
        # 실제 발견된 단계명도 포함
        for stage in summary.keys():
            if stage not in all_stages:
                all_stages.append(stage)
        
        for stage_name in all_stages:
            count = summary.get(stage_name, 0)
            f.write(f"{stage_name:30s}: {count:6d} 개\n")
        
        f.write("-" * 80 + "\n\n")
        
        if file_stats:
            f.write("파일별 제거 현황:\n")
            f.write("-" * 80 + "\n")
            for file_name in sorted(file_stats.keys()):
                f.write(f"\n{file_name}:\n")
                for stage_name, count in sorted(file_stats[file_name].items()):
                    f.write(f"  {stage_name:28s}: {count:6d} 개\n")
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
            end_time = time.time()
            cal_time = end_time - start_time
            print("spent time: ", cal_time, "s")
            print("Done.")


if __name__ == "__main__":
    main()
