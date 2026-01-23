import re
import html
import unicodedata

class EnglishPreprocessing:
    def __init__(self):
        self._name = "english_preproc"

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

        # 2. Apply the base universal preprocessing (logic from former BasePreprocessing)
        if not isinstance(text, str):
            return ""
        if not text or text.isspace():
            return ""
        text = unicodedata.normalize('NFKC', text)
        space_chars = '\u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000'
        for space_char in space_chars:
            text = text.replace(space_char, ' ')
        text = html.unescape(text)
        text = re.sub(r'"+', '"', text)
        text = re.sub(r"'+" , "'", text)
        if text.count('"') % 2 == 1:
            text = text[::-1].replace('"', '', 1)[::-1]
        if text.count("'") % 2 == 1 and text.count("'") > 2:
            text = text[::-1].replace("'", '', 1)[::-1]
        text = re.sub(r'\(\s+', '(', text)
        text = re.sub(r'\s+\)', ')', text)
        text = re.sub(r'\[\s+', '[', text)
        text = re.sub(r'\s+\]', ']', text)
        text = re.sub(r'\(\s*[a-zA-Z]{1}\s*\)', '', text)
        text = re.sub(r'\[\s*[a-zA-Z]{1}\s*\]', '', text)
        text = re.sub(r'\(\s*[;:,.\-!?]\s*\)', '', text)
        text = re.sub(r'\[\s*[;:,.\-!?]\s*\]', '', text)
        text = text.strip()
        if text.startswith('(') and text.endswith(')') and text.count('(') == 1 and text.count(')') == 1:
            text = text[1:-1].strip()
        if text.startswith('[') and text.endswith(']') and text.count('[') == 1 and text.count(']') == 1:
            text = text[1:-1].strip()
        if text.startswith('"') and text.endswith('"') and text.count('"') == 2:
            text = text[1:-1].strip()
        if text.startswith("'") and text.endswith("'") and text.count("'") == 2:
            text = text[1:-1].strip()
        text = re.sub(r'\.{3,}', '…', text)
        text = re.sub(r'\,\.', '.', text)
        text = re.sub(r'\.\,', '.', text)
        text = re.sub(r',(\s*,)+', ',', text)
        text = re.sub(r'!{3,}', '!', text)
        text = re.sub(r'\?{3,}', '?', text)
        text = re.sub(r'\s*[\u318D\u00B7]\s*', '·', text)
        text = re.sub(r'[•ㆍ・ᆞ]', '·', text)
        text = re.sub(' · ', '·', text)
        text = re.sub(' ·', '·', text)
        text = re.sub('· ', '·', text)
        text = re.sub(r' \.(?!\d)', '.', text)
        text = re.sub(r' \,', ',', text)
        text = re.sub(r' \;', ';', text)
        text = re.sub(r' \?', '?', text)
        text = re.sub(r' \!', '!', text)
        text = re.sub(r'\.\?', '?', text)
        text = re.sub(r'\,\?', '?', text)
        text = re.sub(r'\.\!', '!', text)
        text = re.sub(r'\,\!', '!', text)
        text = re.sub(r'\.\s+\?', '?', text)
        text = re.sub(r'\.\s+\!', '!', text)
        text = re.sub(r'\,\s+\?', '?', text)
        text = re.sub(r'\,\s+\!', '!', text)
        text = re.sub(',"', '"', text)
        text = re.sub(r'^\^', '', text)
        text = re.sub(r', \.', '.', text)
        text = re.sub('[„]', '', text)
        text = re.sub('^[,]', '', text)
        text = re.sub('[,]$', '', text)
        text = re.sub('^[;]', '', text)
        text = re.sub('[;]$', '', text)
        text = re.sub('-{2,}', '-', text)
        text = re.sub(r'\u200B', '', text)
        text = re.sub(r'\u200C', '', text)
        text = re.sub(r'\u200D', '', text)
        text = re.sub(r'\uFEFF', '', text)
        text = re.sub(r'\u2060', '', text)
        patterns = {
            "도형 및 화살표": r'[※〓▪♦]',
            "음악 관련 기호": r'[♪♫♬♭♮♯]',
            "카드 및 게임": r'[♠♣♥✪¤]',
            "특수 화살표": r'[ᐅ]',
            "이모지": r'[🏇]',
            "하이픈류": r'[—]',
        }
        for pattern in patterns.values():
            text = re.sub(pattern, '', text)
        text = re.sub('┃', '|', text)
        text = re.sub(r'[\u1100-\u11FF\u3130-\u318F\uA960-\uA97F\uD7B0-\uD7FF]', '', text)
        text = re.sub('[- ]$', '', text)
        text = re.sub('[, ]$', '', text)
        text = re.sub('[ , ]$', '', text)
        text = re.sub(r'[\[\] ]$', '', text)
        text = re.sub('% 1 ', '', text)
        text = re.sub(r'\s{2,}', ' ', text)
        text = text.strip()
        if len(text) < 2 or text in '.,;!?-':
            return ""

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
