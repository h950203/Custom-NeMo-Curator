import re
from .base_preprocessing import BasePreprocessing

class ChinesePreprocessing(BasePreprocessing):
    def __init__(self):
        super().__init__()
        self._name = "chinese_preproc"

    def apply(self, text: str) -> str:
        # TODO: Add Chinese-specific preprocessing logic here.
        # For now, it just uses the parent's apply method.
        return super().apply(text)
