from dataclasses import dataclass, field
from nemo_curator.stages.base import CompositeStage
from nemo_curator.stages.text.modules import Modify, Filter
from nemo_curator.stages.text.utils.custom_utils2 import preprocess_text, calculate_scores, run_filters_on_text

@dataclass 
class PreAnalyzeFilterStage(CompositeStage):
    """
    Filters documents based on multi-lingual quality scores.
    This is a composite stage that combines scoring and filtering.
    """
    filter_config: dict
    lang_code_map: dict
    preprocessors: dict
    name: str = "pre_analyze_filter"

    def __post_init__(self):
        super().__init__()
        self.stages = [
            Modify(
                modifier_fn=self._process_and_score_fields,
                input_fields=[['input', 'src', 'output', 'tgt']],
                output_fields='processing_results'
            ),
            Filter(
                filter_fn=self._final_filter_doc,
                filter_field='processing_results'
            )
        ]

    def decompose(self):
        return self.stages

    def _process_and_score_fields(self, input, src, output, tgt):
        try:
            input_lang = self.lang_code_map.get(src.lower()) if src else None
            output_lang = self.lang_code_map.get(tgt.lower()) if tgt else None

            processed_input = preprocess_text(input, input_lang, self.preprocessors)
            processed_output = preprocess_text(output, output_lang, self.preprocessors)
            
            input_scores = calculate_scores(processed_input, input_lang, self.filter_config)
            output_scores = calculate_scores(processed_output, output_lang, self.filter_config)
            
            return {
                'processed_input': processed_input, 'processed_output': processed_output,
                'input_scores': input_scores, 'output_scores': output_scores,
                'input_lang': input_lang, 'output_lang': output_lang,
            }
        except Exception as e:
            return {'error': str(e)}

    def _final_filter_doc(self, processing_results):
        if not processing_results or 'error' in processing_results:
            return False
        
        input_passed, _, _ = run_filters_on_text(
            processing_results['processed_input'], processing_results['input_lang'], self.filter_config, processing_results['input_scores']
        )
        if not input_passed:
            return False
        
        output_passed, _, _ = run_filters_on_text(
            processing_results['processed_output'], processing_results['output_lang'], self.filter_config, processing_results['output_scores']
        )
        return output_passed
