import re
import unicodedata
import html
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
)
from nemo_curator.stages.text.filters.heuristic_filter import (
    RepeatedLinesFilter,
    SymbolsToWordsFilter,
    UrlsFilter,
    RepeatingDuplicateNGramsFilter,
)
import os
import json

# pp.py 내용 (kaomoji 모듈이 없으므로, 임시로 빈 함수로 대체. 실제로는 kaomoji 라이브러리를 import해야 함)
# 가정: kaomoji.remove_kaomoji는 카오모지를 제거하는 함수. 여기서는 플레이스홀더로 정의.
def remove_kaomoji(text):
    # 실제 구현이 없으므로, 그대로 반환 (필요 시 실제 kaomoji 라이브러리 사용)
    return text

def text_prep(text):
    # 인용 부호 정규화
    text = html.unescape(text)

    # 괄호 정규화 및 불필요한 괄호 제거
    if text.count('(') == text.count(')') and text.count('[') == text.count(']'):
        # 괄호 내부 앞뒤 공백 제거
        text = re.sub(r'\(\s+(.*?)\s+\)', r'(\1)', text)
        text = re.sub(r'\(\s+(.*?)\)', r'(\1)', text)
        text = re.sub(r'\((.*?)\s+\)', r'(\1)', text)

        # 대괄호 내부 앞뒤 공백 제거
        text = re.sub(r'\[\s+(.*?)\s+\]', r'[\1]', text)
        text = re.sub(r'\[\s+(.*?)\]', r'[\1]', text)
        text = re.sub(r'\[(.*?)\s+\]', r'[\1]', text)

        # 단일 알파벳만 포함된 괄호 제거 (목록 표시 등)
        text = re.sub(r'\(\s*[a-zA-Z]{1}\s*\)', '', text)
        text = re.sub(r'\[\s*[a-zA-Z]{1}\s*\]', '', text)

        # 단일 구두점만 포함된 괄호 제거
        text = re.sub(r'\(\s*[;:,.\-!?]\s*\)', '', text)
        text = re.sub(r'\[\s*[;:,.\-!?]\s*\]', '', text)

    # 불필요한 괄호 페어 제거 : 문장의 제일 앞, 뒤가 부호로 되어있는 경우 그 부호를 제거
    text = text.strip()
    if text.startswith('(') and text.endswith(')') and text.count('(') == 1 and text.count(')') == 1:
        text = text[1:-1].strip()
    if text.startswith('[') and text.endswith(']') and text.count('[') == 1 and text.count(']') == 1:
        text = text[1:-1].strip()
    if text.startswith('"') and text.endswith('"') and text.count('"') == 2:
        text = text[1:-1].strip()
    if text.startswith("'") and text.endswith("'") and text.count("'") == 2:
        text = text[1:-1].strip()

    # 인용부호 정리 : 중복 제거
    text = re.sub(r'\"\"+', '"', text)
    text = re.sub(r'\'\'+', "'", text)

    # 구두점 정규화
    text = re.sub(r'\.{3,}', '…', text)
    text = re.sub(r"\,\.", ".", text)
    text = re.sub(r"\.\,", ".", text)
    text = re.sub(r',(\s*,)+', ',', text) # , 후에 , 또는 공백이 연속될 경우 변환

    # 다양한 중간점 기호를 획일화
    text = re.sub(r"\s*[\u318D\u00B7]\s*", "·", text)
    text = re.sub(r"[•ㆍ・ᆞ]", "·", text)
    text = re.sub(" · ", "·", text)
    text = re.sub(" ·", "·", text)
    text = re.sub("· ", "·", text)

    # 불필요한 공백 제거
    text = re.sub(r" \.(?!\d)", ".", text) # 반점 뒤에 숫자가 있는 경우 공백을 없애지 않음
    text = re.sub(" \,", ",", text)
    text = re.sub(" ;", ";", text)
    text = re.sub(" \?", "?", text)
    text = re.sub(" \!", "!", text)

    # 구두점 조합 오류 수정
    text = re.sub("\.\?", "?", text)
    text = re.sub("\,\?", "?", text)
    text = re.sub("\.\!", "!", text)
    text = re.sub("\,\!", "!", text)
    text = re.sub(r'\.\s+\?', "?", text)
    text = re.sub(r'\.\s+\!', "!", text)
    text = re.sub(r'\,\s+\?', "?", text)
    text = re.sub(r'\,\s+\!', "!", text)

    # 오용된 구두점 제거
    text = re.sub(',"', '"', text)

    # 불필요한 특수 문자 제거
    text = re.sub("\^\^", "", text)
    text = re.sub(', \.', '.', text)
    text = re.sub("[„]", "", text)
    text = re.sub("^[:]", "", text)
    text = re.sub("[:]$", "", text)
    text = re.sub("^[,]", "", text)
    text = re.sub("[,]$", "", text)
    text = re.sub("^[;]", "", text)
    text = re.sub("[;]$", "", text)
    text = re.sub("-{2,}", "-", text)

    # ZWS 형태의 공백 제거
    text = re.sub('\u200B', '', text)  # ZERO WIDTH SPACE
    text = re.sub('\u200C', '', text)  # ZERO WIDTH NON-JOINER
    text = re.sub('\u200D', '', text)  # ZERO WIDTH JOINER
    text = re.sub('\uFEFF', '', text)  # ZERO WIDTH NO-BREAK SPACE
    text = re.sub('\u2060', '', text)  # WORD JOINER

    # 기호 제거
    patterns = {
        "도형 및 화살표": r'[※〓▪♦]',
        "음악 관련 기호": r'[♪♫♬♭♮♯]',
        "카드 및 게임": r'[♠♣♥✪¤]',
        "특수 화살표": r'[ᐅ]',
        "이모지": r'[🏇]',
        "하이픈류": r'[—]',
        "오류문자": r'[�]',
    }
    for pattern in patterns.values():
        text = re.sub(pattern, '', text)

    text = re.sub('┃', '|', text)

    # 카오모지 제거
    text = remove_kaomoji(text)

    # 이상한 거 제거
    text = re.sub("[,-]$", "", text)
    text = re.sub("[-]$", "", text)
    text = re.sub("[,]$", "", text)
    text = re.sub("[ ,]$", "", text)
    text = re.sub("[\[]$", "", text)
    text = re.sub("% 1 ", "", text)

    # 공백 정규화
    text = re.sub(r'\s{2,}', ' ', text)

    return text.strip()

# pp_ko.py 내용
def text_prep_ko(text):
    """
    한국어 텍스트에 특화된 전처리 함수
    """
    # 먼저 보편적 전처리 적용
    text = text_prep(text)

    # 쉼표 후에 구두점 추가 (숫자나 인용부호는 제외)
    text = re.sub(r'([^\d\s]),([^\d\s\'"])', r'\1, \2', text)

    # 다양한 인용부호 ASCII 인용 부호로 변환
    text = re.sub(r'[""❝❞“”″‶]', '"', text)
    text = re.sub(r'[‘’‛‵`′]', "'", text)

    text = re.sub(r'（', "(", text)
    text = re.sub(r'）', ")", text)

    # 다양한 괄호 ASCII 괄호로 변환
    text = re.sub(r'❮', "〈", text)
    text = re.sub(r'❯', "〉", text)

    text = re.sub(r'〔', "[", text)
    text = re.sub(r'〕', "]", text)

    # 인명, 지명 마스킹
    text = re.sub("ㅇㅇ씨", "**씨", text)

    # 특정 단어 제거 (완전히 독립된 단어만)
    text = re.sub(r'(?<![가-힣])키키(?![가-힣])', '', text)

    # 2회 이상 반복되는 한글 자음/모음을 완전히 제거
    text = re.sub(r'([ㄱ-ㅎㅏ-ㅣ])\1+', '', text)

    # 불필요한 괄호 내 한자어 처리
    text = re.sub(r'\([^)]*?[\u4e00-\u9fff][^)]*?\)', '', text)
    text = re.sub(r'\[[^\]]*?[\u4e00-\u9fff][^\]]*?\]', '', text)

    # 특정 문구로 시작하는 문장에서 문구 제거
    text = re.sub(r'^음…\s*', '', text)

    return text.strip()

def custom_normalize_whitespace(text):
    """
    Replaces all occurrences of one or more whitespace characters with a single space
    and strips leading/trailing whitespace.
    """
    return re.sub(r'\s+', ' ', text).strip()

def main():
    # Initialize Ray client
    ray_client = RayClient()
    ray_client.start()

    # Create processing pipeline
    pipeline = Pipeline(
        name="quality_filtering_korean_v2.0",
        description="Comprehensive quality filtering and preprocessing for Korean text documents"
    )

    # Load dataset
    input_path = "data/*.jsonl"
    output_path = "filtered_data_v2.1/"
    reader = JsonlReader(file_paths=input_path)
    pipeline.add_stage(reader)

    print("=" * 70)
    print("KOREAN TEXT FILTERING - V2.0 (COMPREHENSIVE)")
    print("=" * 70)
    print("\nActive Preprocessing:")
    print("  ✓ Whitespace Normalization")
    print("  ✓ Korean-Specific Text Preprocessing (text_prep_ko)")
    print("\nActive Filters:")
    print("  ✓ Word Count Filter (min: 5, max: 100000, lang: 'ko')")
    print("  ✓ Histogram Filter (lang: 'ko', threshold: 0.9)")
    print("  ✓ Repeated Lines Filter (min_uniqueness_ratio: 0.7)")
    print("  ✓ Repeating Duplicate NGrams Filter (n=5, max_ratio: 0.2)")
    print("  ✓ Punctuation Filter (max_ratio: 0.85)")
    print("  ✓ Mean Word Length Filter (min: 2, max: 15, lang: 'ko')")
    print("  ✓ Symbols-to-Words Filter (max_ratio: 0.1)")
    print("  ✓ URLs Filter (max_ratio: 0.01)")
    print("\nScoring & Final Filtering:")
    print("  + A composite 'quality_score' will be calculated from multiple metrics.")
    print("  + Documents with 'quality_score' < 0.5 will be removed.")
    print("=" * 70 + "\n")

    # === STAGE 1: PREPROCESSING (TEXT NORMALIZATION) ===
    # 먼저 한국어 특화 전처리 적용 (text_prep_ko)
    korean_preprocessor = Modify(
        modifier_fn=text_prep_ko,
        input_fields="text"
    )
    pipeline.add_stage(korean_preprocessor)

    # 그 다음 일관되지 않은 공백 정리 (기존 whitespace_normalizer)
    whitespace_normalizer = Modify(
        modifier_fn=custom_normalize_whitespace,
        input_fields="text"
    )
    pipeline.add_stage(whitespace_normalizer)

    # === STAGE 2: QUALITY SCORING & FILTERING ===
    # 각 필터는 점수를 생성하며, 이 점수들은 최종 'quality_score' 계산에 사용됩니다.

    # 1. 단어 수 기반 필터링
    word_count_filter = ScoreFilter(
        filter_obj=WordCountFilter(min_words=5, max_words=100000, lang='ko'),
        text_field="text",
        score_field="word_count"
    )
    pipeline.add_stage(word_count_filter)

    # 2. 언어 필터링 (한국어 문서만 통과)
    lang_filter = ScoreFilter(
        filter_obj=HistogramFilter(lang='ko', threshold=0.9),
        text_field="text",
        score_field="lang_score"
    )
    pipeline.add_stage(lang_filter)

    # 3. 반복된 라인 비율 필터링 (고유 라인 비율이 70% 미만인 문서 제거)
    # 점수가 높을수록 좋음 (고유 라인 비율)
    repeated_lines_filter = ScoreFilter(
        filter_obj=RepeatedLinesFilter(max_repeated_line_fraction=0.7),
        text_field="text",
        score_field="repeated_lines_uniqueness_ratio"
    )
    pipeline.add_stage(repeated_lines_filter)
    
    # 4. 반복된 N-gram 비율 필터링 (신규)
    # 점수가 낮을수록 좋음
    repeated_ngram_filter = ScoreFilter(
        filter_obj=RepeatingDuplicateNGramsFilter(n=5, max_repeating_duplicate_ngram_ratio=0.2, lang='ko'),
        text_field="text",
        score_field="repeating_duplicate_ngram_ratio"
    )
    pipeline.add_stage(repeated_ngram_filter)

    # 5. 구두점 비율 필터링
    # 점수가 낮을수록 좋음
    punctuation_filter = ScoreFilter(
        filter_obj=PunctuationFilter(max_num_sentences_without_endmark_ratio=0.85),
        text_field="text",
        score_field="punctuation_ratio"
    )
    pipeline.add_stage(punctuation_filter)
    
    # 6. 평균 단어 길이 필터링
    mean_word_length_filter = ScoreFilter(
        filter_obj=MeanWordLengthFilter(min_mean_word_length=2, max_mean_word_length=15, lang='ko'),
        text_field="text",
        score_field="mean_word_length"
    )
    pipeline.add_stage(mean_word_length_filter)

    # 7. 특수기호 대 단어 비율 필터링 (신규)
    # 점수가 낮을수록 좋음
    symbols = ['#', '...', '“', '”', '…', '@', '%', '$', '^', '&', '*', '(', ')']
    symbols_filter = ScoreFilter(
        filter_obj=SymbolsToWordsFilter(max_symbol_to_word_ratio=0.1, lang='ko'),
        text_field="text",
        score_field="symbol_to_word_ratio"
    )
    pipeline.add_stage(symbols_filter)
    
    # 8. URL 포함 비율 필터링 (신규)
    # 점수가 낮을수록 좋음
    urls_filter = ScoreFilter(
        filter_obj=UrlsFilter(max_url_to_text_ratio=0.01),
        text_field="text",
        score_field="urls_ratio"
    )
    pipeline.add_stage(urls_filter)

    # === STAGE 3: COMPOSITE SCORING (고도화된 품질 점수 계산) ===
    def calculate_quality_score_v2(repeated_lines_uniqueness_ratio, repeating_duplicate_ngram_ratio, punctuation_ratio, symbol_to_word_ratio, urls_ratio):
        # 여러 품질 지표를 종합하여 최종 점수를 계산합니다.
        # 'uniqueness_ratio'는 점수가 높을수록 좋으므로 그대로 사용합니다.
        # 다른 지표들은 '비율'이 낮을수록 좋으므로 (1 - 비율)로 점수를 변환하고, 가중치를 부여합니다.
        # 가중치: 고유 라인(0.2), 중복 N-gram(0.2), 구두점(0.2), 특수기호(0.3), URL(0.1)
        score = (repeated_lines_uniqueness_ratio * 0.2 +
                 (1 - repeating_duplicate_ngram_ratio) * 0.2 +
                 (1 - punctuation_ratio) * 0.2 +
                 (1 - symbol_to_word_ratio) * 0.3 +
                 (1 - urls_ratio) * 0.1)
        return score

    quality_score_stage = Modify(
        modifier_fn=calculate_quality_score_v2,
        input_fields=[["repeated_lines_uniqueness_ratio", "repeating_duplicate_ngram_ratio", "punctuation_ratio", "symbol_to_word_ratio", "urls_ratio"]],
        output_fields="quality_score"
    )
    pipeline.add_stage(quality_score_stage)
    
    # === STAGE 4: FINAL FILTERING (품질 점수 기반 최종 컷오프) ===
    # 계산된 'quality_score'가 0.5 미만인 문서를 최종적으로 제거합니다.
    final_quality_score_threshold_fn = lambda score: score >= 0.5

    final_filter_stage = Filter(
        filter_fn=final_quality_score_threshold_fn,
        filter_field="quality_score"
    )
    pipeline.add_stage(final_filter_stage)

    # Add writer stage
    writer = JsonlWriter(path=output_path)
    pipeline.add_stage(writer)

    # Print pipeline description
    print("\nPipeline Stages:")
    print(pipeline.describe())
    print("\n" + "=" * 70 + "\n")

    # Create executor
    executor = XennaExecutor()

    # Execute pipeline
    print("Starting pipeline execution...")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print("-" * 70)

    try:
        results = pipeline.run(executor)

        # Print results
        print("\n" + "=" * 70)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"\nPipeline Run Stats: {results}")
        print("\nOutput Information:")
        print(f"  - Filtered documents saved to: {output_path}")
        print("  - Each document includes multiple quality scores and a final 'quality_score'")

        # === CALCULATE AND PRINT FILE SCORES ===
        print("\n" + "=" * 70)
        print("AVERAGE QUALITY SCORE PER OUTPUT FILE")
        print("=" * 70)
        if os.path.exists(output_path) and os.path.isdir(output_path):
            output_files = [f for f in os.listdir(output_path) if f.endswith('.jsonl')]
            if not output_files:
                print("No output files found in the output directory.")
            else:
                total_avg_score = 0
                total_files = 0
                for filename in output_files:
                    filepath = os.path.join(output_path, filename)
                    total_score = 0
                    doc_count = 0
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            for line in f:
                                try:
                                    doc = json.loads(line)
                                    if "quality_score" in doc:
                                        total_score += doc["quality_score"]
                                        doc_count += 1
                                except json.JSONDecodeError:
                                    continue  # Skip malformed lines
                        if doc_count > 0:
                            avg_score = total_score / doc_count
                            print(f"  - {filename}: {avg_score:.4f}")
                            total_avg_score += avg_score
                            total_files += 1
                        else:
                            print(f"  - {filename}: No documents with quality score found.")
                    except IOError as e:
                        print(f"Could not read file {filepath}: {e}")
                if total_files > 0:
                    print("-" * 20)
                    print(f"Overall Average Score: {total_avg_score / total_files:.4f}")

        else:
            print(f"Output directory '{output_path}' not found or is not a directory.")

    except Exception as e:
        print("\n" + "=" * 70)
        print("✗ PIPELINE EXECUTION FAILED")
        print("=" * 70)
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print(f"1. Verify input files exist: {input_path}")
        print("2. Check JSONL format: each line should be valid JSON")
        print("3. Ensure 'text' field exists in each document")
        print("4. Check disk space for output directory")

    finally:
        # Stop Ray client
        print("\n" + "-" * 70)
        print("Shutting down Ray cluster...")
        ray_client.stop()
        print("Done.")


if __name__ == "__main__":
    main()
