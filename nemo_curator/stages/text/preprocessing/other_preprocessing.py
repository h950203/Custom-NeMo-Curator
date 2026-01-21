import re
from .base_preprocessing import BasePreprocessing

class OtherPreprocessing(BasePreprocessing):
    def __init__(self):
        super().__init__()
        self._name = "other_preproc"

    def apply(self, text: str) -> str:
        # The full logic now comes from the parent PersonalFilter
        return super().apply(text)

