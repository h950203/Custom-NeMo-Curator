import re
from .base_preprocessing import BasePreprocessing

class JapanesePreprocessing(BasePreprocessing):
    def __init__(self):
        super().__init__()
        self._name = "japanese_preproc"

    def apply(self, text: str) -> str:
        # TODO: Add Japanese-specific preprocessing logic here.
        # For now, it just uses the parent's apply method.
        return super().apply(text)
