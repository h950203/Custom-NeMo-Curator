import re

class OtherPreprocessing:
    def __init__(self):
        self._name = "other_preproc"

    def apply(self, text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return ""
        # Basic whitespace normalization as a default
        return re.sub(r'\s+', ' ', text).strip()

    @property
    def name(self) -> str:
        return self._name

