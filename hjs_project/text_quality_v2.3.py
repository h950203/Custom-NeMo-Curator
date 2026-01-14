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
        name="quality_filtering_korean_v2.3",
        description="Comprehensive quality filtering and preprocessing for Korean text documents with sentence-level custom normalization"
    )

    # Load dataset
    input_path = "data/*.jsonl"
    output_path = "filtered_data_v2.3/"
    reader = JsonlReader(file_paths=input_path)
    pipeline.add_stage(reader)

    print("=" * 70)
    print("KOREAN TEXT FILTERING - V2.2 (COMPREHENSIVE + SENTENCE PREPROC)")
    print("=" * 70)
    print("\nActive Preprocessing:")
    print("  ✓ Sentence Splitting (kss)")
    print("  ✓ Per-Sentence Custom Preprocessing (PersonalPreprocFilter)")
    print("  ✓ Sentence Joining")
    print("  ✓ Whitespace Normalization")
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

    # === STAGE 1: PREPROCESSING (Sentence-level and Document-level) ===

    # STAGE 1A: Sentence Splitting
    def split_into_sentences(text):
        """텍스트를 문장 리스트로 변환"""
        return kss.split_sentences(text)
    
    pipeline.add_stage(Modify(
        modifier_fn=split_into_sentences,
        input_fields="text",
        output_fields="sentences"
    ))

    # STAGE 1B: Per-Sentence Preprocessing
    personal_preprocessor = PersonalFilter()
    
    def process_sentences(sentences):
        """문장 리스트를 처리"""
        processed = [personal_preprocessor.apply(s) for s in sentences]
        return [s for s in processed if s]  # 빈 문장 제거
    
    pipeline.add_stage(Modify(
        modifier_fn=process_sentences,
        input_fields="sentences",
        output_fields="sentences"
    ))

    # STAGE 1C: Sentence Joining
    def join_sentences(sentences):
        """문장 리스트를 텍스트로 결합"""
        return " ".join(sentences) if sentences else ""
    
    pipeline.add_stage(Modify(
        modifier_fn=join_sentences,
        input_fields="sentences",
        output_fields="text"
    ))

    # STAGE 1D: Document-level Whitespace Normalization
    pipeline.add_stage(Modify(
        modifier_fn=custom_normalize_whitespace,
        input_fields="text",
        output_fields="text"
    ))

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
        filter_obj=HistogramFilter(lang='ko', threshold=0.7),
        text_field="text",
        score_field="lang_score"
    )
    pipeline.add_stage(lang_filter)

    # 3. 반복된 라인 비율 필터링
    repeated_lines_filter = ScoreFilter(
        filter_obj=RepeatedLinesFilter(max_repeated_line_fraction=0.7),
        text_field="text",
        score_field="repeated_lines_uniqueness_ratio"
    )
    pipeline.add_stage(repeated_lines_filter)
    
    # 4. 반복된 N-gram 비율 필터링
    repeated_ngram_filter = ScoreFilter(
        filter_obj=RepeatingDuplicateNGramsFilter(n=5, max_repeating_duplicate_ngram_ratio=0.2, lang='ko'),
        text_field="text",
        score_field="repeating_duplicate_ngram_ratio"
    )
    pipeline.add_stage(repeated_ngram_filter)

    # 5. 구두점 비율 필터링
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

    # 7. 특수기호 대 단어 비율 필터링
    symbols_filter = ScoreFilter(
        filter_obj=SymbolsToWordsFilter(max_symbol_to_word_ratio=0.1, lang='ko'),
        text_field="text",
        score_field="symbol_to_word_ratio"
    )
    pipeline.add_stage(symbols_filter)
    
    # 8. URL 포함 비율 필터링
    urls_filter = ScoreFilter(
        filter_obj=UrlsFilter(max_url_to_text_ratio=0.01),
        text_field="text",
        score_field="urls_ratio"
    )
    pipeline.add_stage(urls_filter)

    # === STAGE 3: COMPOSITE SCORING ===
    def calculate_quality_score_v2(repeated_lines_uniqueness_ratio, repeating_duplicate_ngram_ratio, punctuation_ratio, symbol_to_word_ratio, urls_ratio):
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
    
    # === STAGE 4: FINAL FILTERING ===
    final_quality_score_threshold_fn = lambda score: score >= 0.2

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
                                    continue
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
