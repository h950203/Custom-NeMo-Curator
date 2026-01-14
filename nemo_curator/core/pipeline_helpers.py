from collections import defaultdict

from nemo_curator.stages.text.quality.utils import preprocess_text, run_filters_on_text, calculate_scores

def analyze_removed_documents(all_docs, all_input_ids, tracked_output_ids, lang_code_map, preprocessors, config):
    """
    Analyzes documents that were removed by the pipeline to determine the reason for removal.
    """
    removed_docs = defaultdict(list)
    removed_ids = all_input_ids - tracked_output_ids

    for doc in all_docs:
        if doc['_tracking_id'] in removed_ids:
            # Re-simulate the processing to find the removal reason
            input_lang = lang_code_map.get(doc.get('src', '').lower()) if doc.get('src') else None
            output_lang = lang_code_map.get(doc.get('tgt', '').lower()) if doc.get('tgt') else None
            
            processed_input = preprocess_text(doc.get('input', ''), input_lang, preprocessors)
            processed_output = preprocess_text(doc.get('output', ''), output_lang, preprocessors)
            
            input_scores = calculate_scores(processed_input, input_lang, config)
            output_scores = calculate_scores(processed_output, output_lang, config)

            log_entry = {
                **doc,
                'processed_input': processed_input,
                'processed_output': processed_output,
                'input_scores': input_scores,
                'output_scores': output_scores,
            }

            input_passed, input_key, input_reason = run_filters_on_text(
                processed_input, input_lang, config, input_scores
            )
            if not input_passed:
                reason_key = f"input_{input_key}"
                removed_docs[reason_key].append({**log_entry, 'reason': input_reason})
                continue

            output_passed, output_key, output_reason = run_filters_on_text(
                processed_output, output_lang, config, output_scores
            )
            if not output_passed:
                reason_key = f"output_{output_key}"
                removed_docs[reason_key].append({**log_entry, 'reason': output_reason})
                continue
            
            removed_docs['unknown_removal'].append({**log_entry, 'reason': '알 수 없는 이유로 파이프라인에서 누락됨'})

    return removed_docs
