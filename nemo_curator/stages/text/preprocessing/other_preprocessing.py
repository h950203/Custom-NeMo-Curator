import re
from .base_preprocessing import PersonalFilter

class OtherPersonalFilter(PersonalFilter):
    """
    A base filter that applies a series of basic text preprocessing steps.
    Used for languages without a specific filter or as a default.
    Now inherits from PersonalFilter to provide more comprehensive default processing.
    """

    def __init__(self):
        super().__init__()
        self._name = "other_personal_preproc_filter"

    def apply(self, text: str) -> str:
        # The full logic now comes from the parent PersonalFilter
        return super().apply(text)

