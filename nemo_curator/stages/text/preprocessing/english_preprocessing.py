import re
from .personal_preprocessing import PersonalFilter

class EnglishPersonalFilter(PersonalFilter):
    """
    A filter that applies a series of English-specific text preprocessing steps,
    building upon the base PersonalFilter.
    """
    def __init__(self):
        super().__init__()
        self._name = "english_personal_preproc_filter"

    def apply(self, text: str) -> str:
        if not isinstance(text, str):
            return ""

        # 1. Apply HTML cleaning from pp_en.py's text_prep_html
        # HTML 태그 제거 (< > 안의 모든 내용)
        text = re.sub(r'<[^>]*>', '', text)

        # 괄호 안의 내용 제거
        max_iterations = 10  # 무한루프 방지
        iteration = 0
        while '(' in text and ')' in text and iteration < max_iterations:
            prev_text = text
            text = re.sub(r'\([^()]*\)', '', text)
            if text == prev_text: break
            iteration += 1
        
        iteration = 0
        while '[' in text and ']' in text and iteration < max_iterations:
            prev_text = text
            text = re.sub(r'\[[^\[\]]*\]', '', text)
            if text == prev_text: break
            iteration += 1
        
        iteration = 0
        while '{' in text and '}' in text and iteration < max_iterations:
            prev_text = text
            text = re.sub(r'\{[^{}]*\}', '', text)
            if text == prev_text: break
            iteration += 1

        # 남은 괄호들 제거
        text = re.sub(r'[(){}\[\]]', '', text)

        # 2. Apply the base universal preprocessing by calling parent's apply
        text = super().apply(text)

        # 3. Apply English-specific preprocessing from pp_en.py's text_prep_en
        text = re.sub(r'[""❝❞“”″‶]', '"', text)
        text = re.sub(r'[‘’‛‵`′]', "'", text)

        # 괄호 정규화
        text = re.sub(r'❮', "〈", text)
        text = re.sub(r'❯', "〉", text)
        text = re.sub(r'〔', "[", text)
        text = re.sub(r'〕', "]", text)

        # 영어 직함 약어 처리
        punc_words = r"(Mr|Mrs|Ms|Prof|Rev|Fr|Sr|St|Dr|no)\.\s+"
        text = re.sub(punc_words, r"\1.", text)

        # 영어 약어 처리
        text = re.sub(r"vs\.(\S)?", r"vs.\1", text)

        # 영어 숫자 표현 정규화
        text = re.sub(r'(\d);(\d)', r'\1:\2', text)
        
        # Final whitespace normalization
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    @property
    def name(self) -> str:
        return self._name
