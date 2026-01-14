import os
from collections import defaultdict

def print_filter_config(config, pipeline_version):
    """Prints the filter configuration."""
    print("\n" + "=" * 70)
    print(f"MULTI-LINGUAL TEXT FILTERING - {pipeline_version}")
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

def save_global_logs(logs_dir, global_removed_docs):
    """Saves the logs of removed documents, categorized by removal reason."""
    os.makedirs(logs_dir, exist_ok=True)
    print(f"\n로그 파일 생성 중...")
    
    for stage_name, docs in global_removed_docs.items():
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

def save_preprocessing_logs(logs_dir, global_preprocessing_logs):
    """Saves logs of modified sentences during preprocessing."""
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, "preprocessing.txt")
    print(f"\n전처리 로그 파일 생성 중...")
    if not global_preprocessing_logs:
        print("  - 변경된 문장 없음.")
        return
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"전처리 중 변경된 문장 목록\n총 변경된 문장 수: {len(global_preprocessing_logs)}\n" + "=" * 80 + "\n\n")
        sorted_logs = sorted(global_preprocessing_logs, key=lambda x: (x['_source_file'], x['_line_num']))
        last_file = None
        for log in sorted_logs:
            if last_file != log['_source_file']:
                f.write(f"\n\n{'='*80}\n파일: {log['_source_file']}\n{'='*80}\n")
                last_file = log['_source_file']
            f.write(f"\n---\n라인: {log['_line_num']}\n필드: {log['field']}\n원본: {log['original']}\n수정: {log['modified']}\n")
    print(f"  ✓ preprocessing.txt ({len(global_preprocessing_logs)}개 문장)")

def create_summary(logs_dir, global_removed_docs, pipeline_version):
    """Creates a summary file of the filtering results."""
    summary = {stage: len(docs) for stage, docs in global_removed_docs.items()}
    file_stats = defaultdict(lambda: defaultdict(int))
    for stage, docs in global_removed_docs.items():
        for doc in docs:
            file_stats[doc['_source_file']][stage] += 1
    
    summary_file = os.path.join(logs_dir, "summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"필터링 요약 - {pipeline_version}\n" + "=" * 80 + "\n\n")
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
