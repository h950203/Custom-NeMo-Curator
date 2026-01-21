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
            ),
        ]

    def decompose(self):
        return self.stages

    def _filter_sentences(self, doc):
        if 'processing_results' not in doc or not doc['processing_results']:
            return doc

        proc_results = doc['processing_results']
        
        # Filter input sentences
        if 'input_analysis' in proc_results and proc_results['input_analysis']:
            kept_input_sentences = []
            for sent_analysis in proc_results['input_analysis']:
                text = sent_analysis.get('text', '')
                main_clauses = sent_analysis.get('main_clauses', 0)
                avg_token_length = sent_analysis.get('avg_token_length', 0)
                content_word_ratio = sent_analysis.get('content_word_ratio', 0)

                if not (main_clauses == 0 or not (1.5 <= avg_token_length <= 15) or content_word_ratio < 20):
                    kept_input_sentences.append(text)
            
            proc_results['processed_input'] = ' '.join(kept_input_sentences)

        # Filter output sentences
        if 'output_analysis' in proc_results and proc_results['output_analysis']:
            kept_output_sentences = []
            for sent_analysis in proc_results['output_analysis']:
                text = sent_analysis.get('text', '')
                main_clauses = sent_analysis.get('main_clauses', 0)
                avg_token_length = sent_analysis.get('avg_token_length', 0)
                content_word_ratio = sent_analysis.get('content_word_ratio', 0)

                if not (main_clauses == 0 or not (1.5 <= avg_token_length <= 15) or content_word_ratio < 20):
                    kept_output_sentences.append(text)

            proc_results['processed_output'] = ' '.join(kept_output_sentences)

        doc['processing_results'] = proc_results
        
        return doc
