import re


class KoreanPreprocessing:
    """
    Korean-specific preprocessing.
    Applied AFTER BasePreprocessing for Korean language texts only.
    Focuses on Korean-specific characters and patterns.
    """
    def __init__(self):
        self._name = "korean_preproc"

    def apply(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        
        # 빈 문자열이나 공백만 있는 경우 조기 반환
        if not text or text.isspace():
            return ""
        
        # 한글 중간점 기호를 획일화 (한국어 특화)
        text = re.sub(r'\s*[\u318D\u00B7]\s*', '·', text)
        text = re.sub(r'[•ㆍ・ᆞ]', '·', text)
        text = re.sub(' · ', '·', text)
        text = re.sub(' ·', '·', text)
        text = re.sub('· ', '·', text)
        
        # 한글 자모 분리 문자 제거 (한국어 특화)
        text = re.sub(r'[\u1100-\u11FF\u3130-\u318F\uA960-\uA97F\uD7B0-\uD7FF]', '', text)
        
        # 한국어 특화 이상 패턴 제거
        text = re.sub('% 1 ', '', text)
        
        # 공백 정규화
        text = re.sub(r'\s{2,}', ' ', text)
        
        text = text.strip()
        
        # 최종 검증: 의미있는 내용이 있는지 확인
        if len(text) < 2 or text in '.,;!?-':
            return ""

        return text

    @property
    def name(self) -> str:
        return self._name