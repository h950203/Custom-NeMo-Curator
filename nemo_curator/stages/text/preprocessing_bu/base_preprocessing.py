import re

class BasePersonalFilter:
    """
    A base filter that applies a series of basic text preprocessing steps.
    Used for languages without a specific filter or as a default.
    """
    def __init__(self):
        self._name = "base_personal_preproc_filter"

    def apply(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        # Basic whitespace normalization
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @property
    def name(self) -> str:
        return self._name
