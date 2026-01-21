from dataclasses import dataclass
from nemo_curator.stages.base import CompositeStage
from nemo_curator.stages.text.modules import Modify

@dataclass
class PostAnalyzeFilterStage(CompositeStage):
    """
    Filters sentences based on analysis scores.
    """
    name: str = "post_analyze_filter"

    def __post_init__(self):
        super().__init__()
        self.stages = [
            Modify(
                modifier_fn=self._filter_sentences,
                input_fields="processing_results",
                output_fields="processing_results",
            ),
        ]

    def decompose(self):
        return self.stages

    def _filter_sentences(self, proc_results):
        if not isinstance(proc_results, dict):
            return proc_results

        new_proc_results = proc_results.copy()
        
        # Filter input sentences
        if 'input_analysis' in new_proc_results and new_proc_results['input_analysis']:
            kept_input_sentences = []
            for sent_analysis in new_proc_results['input_analysis']:
                text = sent_analysis.get('text', '')
                try:
                    main_clauses = sent_analysis.get('main_clauses')
                    avg_token_length = sent_analysis.get('avg_token_length')
                    content_word_ratio = sent_analysis.get('content_word_ratio')

                    # Keep sentence if any score is missing
                    if main_clauses is None or avg_token_length is None or content_word_ratio is None:
                        kept_input_sentences.append(text)
                        continue

                    # Keep sentence if it does NOT meet any of the removal criteria
                    if not (main_clauses == 0 or not (1.5 <= avg_token_length <= 15) or content_word_ratio < 20):
                        kept_input_sentences.append(text)
                except TypeError:
                    # Keep sentence if a comparison fails
                    kept_input_sentences.append(text)
            
            new_proc_results['processed_input'] = ' '.join(kept_input_sentences)

        # Filter output sentences
        if 'output_analysis' in new_proc_results and new_proc_results['output_analysis']:
            kept_output_sentences = []
            for sent_analysis in new_proc_results['output_analysis']:
                text = sent_analysis.get('text', '')
                try:
                    main_clauses = sent_analysis.get('main_clauses')
                    avg_token_length = sent_analysis.get('avg_token_length')
                    content_word_ratio = sent_analysis.get('content_word_ratio')

                    # Keep sentence if any score is missing
                    if main_clauses is None or avg_token_length is None or content_word_ratio is None:
                        kept_output_sentences.append(text)
                        continue

                    # Keep sentence if it does NOT meet any of the removal criteria
                    if not (main_clauses == 0 or not (1.5 <= avg_token_length <= 15) or content_word_ratio < 20):
                        kept_output_sentences.append(text)
                except TypeError:
                    # Keep sentence if a comparison fails
                    kept_output_sentences.append(text)

            new_proc_results['processed_output'] = ' '.join(kept_output_sentences)
        
        return new_proc_results
