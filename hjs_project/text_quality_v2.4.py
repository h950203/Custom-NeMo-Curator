from nemo_curator.core.client import RayClient
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.modules import ScoreFilter, Modify, Filter
from nemo_curator.stages.text.filters import (
    WordCountFilter,
    HistogramFilter,
    PunctuationFilter,
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
    'max_punctuation_ratio': 0.85,
    'min_mean_word_length': 2,
    'max_mean_word_length': 15,
    'max_symbol_to_word_ratio': 0.05,
    'max_url_ratio': 0.01,
    'min_quality_score': 0.05,
    'quality_weights': {
        'repeated_lines': 0.2,
        'repeated_ngrams': 0.2,
        'punctuation': 0.2,
        'symbols': 0.3,
        'urls': 0.1
    }
}

INPUT_PATH = "data/"
OUTPUT_PATH = "filtered_data_v2.4/"
REMOVED_LOGS_PATH = "filtered_data_v2.4/log/"
PIPELINE_VERSION = "v2.4"


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
    print(f"  • 구두점 비율: ≤ {config['max_punctuation_ratio']}")
    print(f"  • 평균 단어 길이: {config['min_mean_word_length']} ~ {config['max_mean_word_length']}")
    print(f"  • 특수기호/단어 비율: ≤ {config['max_symbol_to_word_ratio']}")
    print(f"  • URL 비율: ≤ {config['max_url_ratio']}")
    print(f"  • 최종 품질 점수: ≥ {config['min_quality_score']}")
    print("=" * 70)


def process_file_with_tracking(input_file, output_dir, logs_dir, config):
    """개별 파일을 처리하고 제거된 문서 추적"""
    
    filename = os.path.basename(input_file)
    print(f"\n처리 중: {filename}")
    
    # 제거된 문서 추적
    removed_docs = defaultdict(list)
    
    # 입력 파일 읽기
    all_docs = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line)
                    doc['_line_num'] = line_num
                    all_docs.append(doc)
                except json.JSONDecodeError:
                    continue
    except IOError as e:
        print(f"  ✗ 파일 읽기 오류: {e}")
        return
    
    total_input = len(all_docs)
    print(f"  입력: {total_input}개 문서")
    
    # 파이프라인 생성
    pipeline = Pipeline(
        name=f"filter_{filename}",
        description=f"Processing {filename}"
    )
    
    # 임시 입력/출력 경로
    temp_input = f"/tmp/temp_input_{filename}"
    temp_output = f"/tmp/temp_output_{filename}/"
    
    # 임시 파일에 쓰기
    with open(temp_input, 'w', encoding='utf-8') as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')
    
    reader = JsonlReader(file_paths=temp_input)
    pipeline.add_stage(reader)
    
    # === STAGE 1: PREPROCESSING ===
    
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
    
    # === STAGE 2: QUALITY SCORING ===
    
    filters_config = [
        ("word_count", WordCountFilter(min_words=config['min_words'], 
                                      max_words=config['max_words'], lang='ko'), "word_count"),
        ("language", HistogramFilter(lang='ko', threshold=config['lang_threshold']), "lang_score"),
        ("repeated_lines", RepeatedLinesFilter(max_repeated_line_fraction=config['max_repeated_line_fraction']), 
         "repeated_lines_uniqueness_ratio"),
        ("repeated_ngrams", RepeatingDuplicateNGramsFilter(n=config['ngram_n'], 
                                                          max_repeating_duplicate_ngram_ratio=config['max_repeating_ngram_ratio'], 
                                                          lang='ko'), "repeating_duplicate_ngram_ratio"),
        ("punctuation", PunctuationFilter(max_num_sentences_without_endmark_ratio=config['max_punctuation_ratio']), 
         "punctuation_ratio"),
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
    
    # === STAGE 3: COMPOSITE SCORING ===
    weights = config['quality_weights']
    
    def calculate_quality_score_v2(repeated_lines_uniqueness_ratio, repeating_duplicate_ngram_ratio, 
                                   punctuation_ratio, symbol_to_word_ratio, urls_ratio):
        score = (repeated_lines_uniqueness_ratio * weights['repeated_lines'] +
                 (1 - repeating_duplicate_ngram_ratio) * weights['repeated_ngrams'] +
                 (1 - punctuation_ratio) * weights['punctuation'] +
                 (1 - symbol_to_word_ratio) * weights['symbols'] +
                 (1 - urls_ratio) * weights['urls'])
        return score
    
    quality_score_stage = Modify(
        modifier_fn=calculate_quality_score_v2,
        input_fields=[["repeated_lines_uniqueness_ratio", "repeating_duplicate_ngram_ratio", 
                      "punctuation_ratio", "symbol_to_word_ratio", "urls_ratio"]],
        output_fields="quality_score"
    )
    pipeline.add_stage(quality_score_stage)
    
    # === STAGE 4: FINAL FILTERING ===
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
    
    try:
        pipeline.run(executor)
        
        # 출력된 문서 읽기
        output_docs = []
        output_files = glob.glob(os.path.join(temp_output, "*.jsonl"))
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        doc = json.loads(line)
                        output_docs.append(doc)
                    except json.JSONDecodeError:
                        continue
        
        # 최종 출력 파일 생성 (원본 파일명 유지)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for doc in output_docs:
                # 내부 필드 제거
                doc.pop('_line_num', None)
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        # 제거된 문서 분석
        output_line_nums = {doc.get('_line_num') for doc in output_docs}
        
        personal_preprocessor = PersonalFilter()
        
        for original_doc in all_docs:
            line_num = original_doc['_line_num']
            
            if line_num in output_line_nums:
                continue  # 살아남은 문서
            
            # 제거된 문서 분석
            text = original_doc.get('text', '')
            
            if not text:
                removed_docs['preprocessing_empty'].append({
                    'line_num': line_num,
                    'text': text,
                    'reason': '빈 텍스트'
                })
                continue
            
            # 전처리 시뮬레이션
            try:
                sentences = kss.split_sentences(text)
                processed_sentences = [personal_preprocessor.apply(s) for s in sentences]
                processed_sentences = [s for s in processed_sentences if s]
                
                if not processed_sentences:
                    removed_docs['preprocessing_empty'].append({
                        'line_num': line_num,
                        'text': text[:500],
                        'reason': '전처리 후 빈 문장'
                    })
                    continue
                
                processed_text = " ".join(processed_sentences)
                processed_text = custom_normalize_whitespace(processed_text)
                
            except Exception as e:
                removed_docs['preprocessing_empty'].append({
                    'line_num': line_num,
                    'text': text[:500],
                    'reason': f'전처리 오류: {str(e)[:100]}'
                })
                continue
            
            # 필터 기준 체크
            words = processed_text.split()
            word_count = len(words)
            
            # 단어 수
            if word_count < config['min_words'] or word_count > config['max_words']:
                removed_docs['word_count'].append({
                    'line_num': line_num,
                    'text': text[:500],
                    'processed_text': processed_text[:500],
                    'reason': f'단어 수: {word_count}'
                })
                continue
            
            # 한국어 비율
            korean_chars = len(re.findall(r'[가-힣]', processed_text))
            total_chars = len(re.sub(r'\s', '', processed_text))
            if total_chars > 0:
                korean_ratio = korean_chars / total_chars
                if korean_ratio < config['lang_threshold']:
                    removed_docs['language'].append({
                        'line_num': line_num,
                        'text': text[:500],
                        'processed_text': processed_text[:500],
                        'reason': f'한국어 비율: {korean_ratio:.2f}'
                    })
                    continue
            
            # 나머지는 품질 점수로 추정
            removed_docs['quality_score'].append({
                'line_num': line_num,
                'text': text[:500],
                'processed_text': processed_text[:500],
                'reason': '품질 점수 미달'
            })
        
        # 제거 로그 저장
        os.makedirs(logs_dir, exist_ok=True)
        
        for stage_name, docs in removed_docs.items():
            if not docs:
                continue
            
            log_file = os.path.join(logs_dir, f"{filename.replace('.jsonl', '')}_{stage_name}.txt")
            
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"=" * 80 + "\n")
                f.write(f"파일: {filename}\n")
                f.write(f"제거 단계: {stage_name}\n")
                f.write(f"제거된 문서 수: {len(docs)}\n")
                f.write(f"=" * 80 + "\n\n")
                
                for doc_info in docs:
                    f.write(f"\n{'='*80}\n")
                    f.write(f"라인 번호: {doc_info['line_num']}\n")
                    f.write(f"제거 이유: {doc_info['reason']}\n")
                    f.write(f"\n원본 텍스트:\n{doc_info['text']}\n")
                    if 'processed_text' in doc_info:
                        f.write(f"\n처리된 텍스트:\n{doc_info['processed_text']}\n")
                    f.write("\n")
        
        total_removed = sum(len(docs) for docs in removed_docs.values())
        total_output = len(output_docs)
        
        print(f"  출력: {total_output}개 문서")
        print(f"  제거: {total_removed}개 문서 ({total_removed/total_input*100:.1f}%)")
        
        if removed_docs:
            print(f"  제거 상세:")
            for stage_name, docs in removed_docs.items():
                if docs:
                    print(f"    - {stage_name}: {len(docs)}개")
        
    except Exception as e:
        print(f"  ✗ 처리 실패: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 임시 파일 정리
        if os.path.exists(temp_input):
            os.remove(temp_input)
        if os.path.exists(temp_output):
            import shutil
            shutil.rmtree(temp_output)


def create_summary(logs_dir):
    """전체 요약 파일 생성"""
    
    log_files = glob.glob(os.path.join(logs_dir, "*_*.txt"))
    
    summary = defaultdict(int)
    file_stats = defaultdict(lambda: defaultdict(int))
    
    for log_file in log_files:
        basename = os.path.basename(log_file)
        
        # 파일명에서 원본 파일명과 단계명 분리
        # 예: output_quality_score.txt -> output_quality.jsonl, score
        parts = basename.replace('.txt', '').split('_')
        
        if len(parts) < 2:
            continue
        
        # 마지막 부분이 단계명
        stage_name = parts[-1]
        # 나머지가 파일명
        file_prefix = '_'.join(parts[:-1])
        
        # 로그 파일에서 제거된 문서 수 읽기
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('제거된 문서 수:'):
                    count = int(line.split(':')[1].strip().split()[0])
                    summary[stage_name] += count
                    file_stats[f"{file_prefix}.jsonl"][stage_name] = count
                    break
    
    summary_file = os.path.join(logs_dir, "removal_summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"=" * 80 + "\n")
        f.write(f"필터링 요약 - {PIPELINE_VERSION}\n")
        f.write(f"=" * 80 + "\n\n")
        
        total_removed = sum(summary.values())
        f.write(f"총 제거된 문서 수: {total_removed}\n\n")
        
        f.write("단계별 제거 현황:\n")
        f.write("-" * 80 + "\n")
        
        # 모든 단계를 정의된 순서대로 표시
        all_stages = ['preprocessing_empty', 'word_count', 'language', 'repeated_lines', 
                     'repeated_ngrams', 'punctuation', 'mean_word_length', 'symbols', 'urls', 'quality_score']
        
        # 실제 발견된 단계명도 포함 (정의되지 않은 이름들)
        for stage in summary.keys():
            if stage not in all_stages:
                all_stages.append(stage)
        
        for stage_name in all_stages:
            count = summary.get(stage_name, 0)
            # 단계명 매핑 (축약된 이름을 전체 이름으로)
            display_name = stage_name
            if stage_name == 'count':
                display_name = 'word_count'
            elif stage_name == 'score':
                display_name = 'quality_score'
            
            f.write(f"{display_name:30s}: {count:6d} 개\n")
        
        f.write("-" * 80 + "\n\n")
        
        f.write("파일별 제거 현황:\n")
        f.write("-" * 80 + "\n")
        for file_name in sorted(file_stats.keys()):
            f.write(f"\n{file_name}:\n")
            for stage_name, count in sorted(file_stats[file_name].items()):
                # 단계명 매핑
                display_name = stage_name
                if stage_name == 'count':
                    display_name = 'word_count'
                elif stage_name == 'score':
                    display_name = 'quality_score'
                f.write(f"  {display_name:28s}: {count:6d} 개\n")
        f.write("-" * 80 + "\n")
    
    print(f"\n✓ 요약 파일 생성: {summary_file}")


def main():
    # Initialize Ray client
    ray_client = RayClient()
    ray_client.start()

    # 현재 설정 출력
    print_filter_config(FILTER_CONFIG)

    print("\nStarting file-by-file processing...")
    print(f"Input: {INPUT_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Logs: {REMOVED_LOGS_PATH}")
    print("-" * 70)

    try:
        # 입력 파일 목록
        input_files = glob.glob(os.path.join(INPUT_PATH, "*.jsonl"))
        
        if not input_files:
            print(f"✗ {INPUT_PATH}에 jsonl 파일이 없습니다.")
            return
        
        print(f"\n발견된 파일: {len(input_files)}개")
        
        # 각 파일 처리
        for input_file in sorted(input_files):
            process_file_with_tracking(input_file, OUTPUT_PATH, REMOVED_LOGS_PATH, FILTER_CONFIG)
        
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
        print("\n" + "-" * 70)
        print("Shutting down Ray cluster...")
        ray_client.stop()
        print("Done.")


if __name__ == "__main__":
    main()
