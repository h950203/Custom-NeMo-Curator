import re
import html
import unicodedata

class BasePreprocessing:
    def __init__(self):
        self._name = "base_preproc"

    def apply(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        
        # 빈 문자열이나 공백만 있는 경우 조기 반환
        if not text or text.isspace():
            return ""
        
        # 유니코드 정규화 (전각->반각 변환 포함)
        text = unicodedata.normalize('NFKC', text)
        
        # 다양한 유니코드 공백 문자를 일반 공백으로 통일
        space_chars = '\u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000'
        for space_char in space_chars:
            text = text.replace(space_char, ' ')
        
        # 인용 부호 정규화
        text = html.unescape(text)

        # 인용부호 중복 제거
        text = re.sub(r'"+', '"', text)
        text = re.sub(r"'+", "'", text)
        
        # 홀수 개의 인용부호 처리 (마지막 것 제거)
        if text.count('"') % 2 == 1:
            text = text[::-1].replace('"', '', 1)[::-1]
        if text.count("'") % 2 == 1 and text.count("'") > 2:
            text = text[::-1].replace("'", '', 1)[::-1]

        # 괄호 정규화 및 불필요한 괄호 제거
        # 괄호 내부 앞뒤 공백 제거
        text = re.sub(r'\(\s+', '(', text)
        text = re.sub(r'\s+\)', ')', text)
        text = re.sub(r'\[\s+', '[', text)
        text = re.sub(r'\s+\]', ']', text)

        # 단일 알파벳만 포함된 괄호 제거 (목록 표시 등)
        text = re.sub(r'\(\s*[a-zA-Z]{1}\s*\)', '', text)
        text = re.sub(r'\[\s*[a-zA-Z]{1}\s*\]', '', text)

        # 단일 구두점만 포함된 괄호 제거
        text = re.sub(r'\(\s*[;:,.\-!?]\s*\)', '', text)
        text = re.sub(r'\[\s*[;:,.\-!?]\s*\]', '', text)

        # 불필요한 괄호 페어 제거 : 문장의 제일 앞, 뒤가 부호로 되어있는 경우 그 부호를 제거
        text = text.strip()
        if text.startswith('(') and text.endswith(')') and text.count('(') == 1 and text.count(')') == 1:
            text = text[1:-1].strip()
        if text.startswith('[') and text.endswith(']') and text.count('[') == 1 and text.count(']') == 1:
            text = text[1:-1].strip()
        if text.startswith('"') and text.endswith('"') and text.count('"') == 2:
            text = text[1:-1].strip()
        if text.startswith("'") and text.endswith("'") and text.count("'") == 2:
            text = text[1:-1].strip()

        # 구두점 정규화
        text = re.sub(r'\.{3,}', '…', text)
        text = re.sub(r'\,\.', '.', text)
        text = re.sub(r'\.\,', '.', text)
        text = re.sub(r',(\s*,)+', ',', text)
        
        # 연속된 구두점 정리 (!!!, ??? 등을 !!,?? 로)
        text = re.sub(r'!{3,}', '!', text)
        text = re.sub(r'\?{3,}', '?', text)

        # 다양한 중간점 기호를 획일화
        text = re.sub(r'\s*[\u318D\u00B7]\s*', '·', text)
        text = re.sub(r'[•ㆍ・ᆞ]', '·', text)
        text = re.sub(' · ', '·', text)
        text = re.sub(' ·', '·', text)
        text = re.sub('· ', '·', text)

        # 불필요한 공백 제거
        text = re.sub(r' \.(?!\d)', '.', text) # 반점 뒤에 숫자가 있는 경우 공백을 없애지 않음
        text = re.sub(r' \,', ',', text)
        text = re.sub(r' \;', ';', text)
        text = re.sub(r' \?', '?', text)
        text = re.sub(r' \!', '!', text)

        # 구두점 조합 오류 수정
        text = re.sub(r'\.\?', '?', text)
        text = re.sub(r'\,\?', '?', text)
        text = re.sub(r'\.\!', '!', text)
        text = re.sub(r'\,\!', '!', text)
        text = re.sub(r'\.\s+\?', '?', text)
        text = re.sub(r'\.\s+\!', '!', text)
        text = re.sub(r'\,\s+\?', '?', text)
        text = re.sub(r'\,\s+\!', '!', text)

        # 오용된 구두점 제거
        text = re.sub(',"', '"', text)

        # 불필요한 특수 문자 제거
        text = re.sub(r'^\^', '', text)
        text = re.sub(r', \.', '.', text)
        text = re.sub('[„]', '', text)
        text = re.sub('^[,]', '', text)
        text = re.sub('[,]$', '', text)
        text = re.sub('^[;]', '', text)
        text = re.sub('[;]$', '', text)
        text = re.sub('-{2,}', '-', text)

        # ZWS 형태의 공백 제거
        text = re.sub(r'\u200B', '', text)  # ZERO WIDTH SPACE
        text = re.sub(r'\u200C', '', text)  # ZERO WIDTH NON-JOINER
        text = re.sub(r'\u200D', '', text)  # ZERO WIDTH JOINER
        text = re.sub(r'\uFEFF', '', text)  # ZERO WIDTH NO-BREAK SPACE
        text = re.sub(r'\u2060', '', text)  # WORD JOINER

        # 기호 제거
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
        
        # 한글 자모 분리 문자 제거
        text = re.sub(r'[\u1100-\u11FF\u3130-\u318F\uA960-\uA97F\uD7B0-\uD7FF]', '', text)

        # 이상한 거 제거
        text = re.sub('[- ]$', '', text)
        text = re.sub('[, ]$', '', text)
        text = re.sub('[ , ]$', '', text)
        text = re.sub(r'[\[\] ]$', '', text)
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
