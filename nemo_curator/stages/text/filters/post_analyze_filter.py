from nemo_curator.stages.text.modules import Modify

def _filter_sentences(proc_results):
    if not isinstance(proc_results, dict):
        return proc_results

    new_proc_results = proc_results.copy()

    def _filter_analysis_list(analysis_list):
        if not analysis_list:
            return ""
        
        kept_sentences = []
        for sent_analysis in analysis_list:
            text = sent_analysis.get('text', '')
            try:
                main_clauses = sent_analysis.get('main_clauses')
                avg_token_length = sent_analysis.get('avg_token_length')
                content_word_ratio = sent_analysis.get('content_word_ratio')

                if main_clauses is None or avg_token_length is None or content_word_ratio is None:
                    kept_sentences.append(text)
                    continue

                remove_sentence = (
                    main_clauses == 0 or
                    not (1.5 <= avg_token_length <= 15) or
                    content_word_ratio < 20
                )
                if not remove_sentence:
                    kept_sentences.append(text)
            except (TypeError, ValueError):
                kept_sentences.append(text)
        return ' '.join(kept_sentences)

    if 'input_analysis' in new_proc_results:
        new_proc_results['processed_input'] = _filter_analysis_list(new_proc_results.get('input_analysis'))

    if 'output_analysis' in new_proc_results:
        new_proc_results['processed_output'] = _filter_analysis_list(new_proc_results.get('output_analysis'))
    
    return new_proc_results

def PostAnalyzeFilterStage():
    """
    Factory function that returns a Modify stage for filtering sentences.
    """
    return Modify(
        modifier_fn=_filter_sentences,
        input_fields="processing_results",
        output_fields="processing_results",
        name="post_analyze_filter",
    )
