import re
import kss
import html
import unicodedata

# ============================================================================
# Kaomoji Filter Logic (from nemo_curator/stages/text/filters/kaomoji.py)
# ============================================================================
def _remove_custom_kaomoji(text):
    text = re.sub(r"φ（＾ω＾）", "", text)
    text = re.sub(r"（＾ω＾）", "", text)
    text = re.sub(r'\(\^\^ゞ', "", text)
    text = re.sub(r"φ（＾ω＾）", "", text)

    return text

def _remove_kaomoji(text):
    text = _remove_custom_kaomoji(text)

    # 1. 기본 표정 카오모지
    text = re.sub(r"\^\^", "", text)
    text = re.sub(r"\(\^_\^\)", "", text)
    text = re.sub(r"\(\^\^\)", "", text)
    text = re.sub(r"n_n", "", text)
    text = re.sub(r"\*\^_\^\*", "", text)
    text = re.sub(r"\^_\^", "", text)
    text = re.sub(r"\^\^_\^\^", "", text)
    text = re.sub(r"\^,\^", "", text)
    text = re.sub(r"\^o\^", "", text)
    text = re.sub(r"XuX", "", text)

    # 2. 슬픈 표정
    text = re.sub(r"é\.è", "", text)
    text = re.sub(r"é_è", "", text)

    # 3. 무표정/지루한 표정
    text = re.sub(r"¬_¬\"", "", text)
    text = re.sub(r"u_u", "", text)
    text = re.sub(r"U_U'", "", text)
    text = re.sub(r"<_<'", "", text)
    text = re.sub(r"-_-\"", "", text)
    text = re.sub(r"¬~¬\"", "", text)
    text = re.sub(r"=_=", "", text)
    text = re.sub(r"--\"", "", text)
    text = re.sub(r"-\.-\"", "", text)

    # 4. 윙크
    text = re.sub(r"\^_-", "", text)
    text = re.sub(r"\(o\.~\)", "", text)
    text = re.sub(r"\(\^_-\)", "", text)
    text = re.sub(r"\(\^\.~\)", "", text)
    text = re.sub(r"\('_-\)", "", text)
    text = re.sub(r"OwU", "", text)
    text = re.sub(r"O_U", "", text)

    # 5. 혀 내밀기
    text = re.sub(r"XpX", "", text)
    text = re.sub(r"OpO", "", text)

    # 6. 놀란 표정
    text = re.sub(r"\(@_@\)", "", text)
    text = re.sub(r"O_o", "", text)
    text = re.sub(r"o_O", "", text)
    text = re.sub(r"°°", "", text)
    text = re.sub(r"°o°", "", text)
    text = re.sub(r"o__Ô", "", text)
    text = re.sub(r"ô__O", "", text)
    text = re.sub(r"-_o", "", text)
    text = re.sub(r"OoO", "", text)
    text = re.sub(r"Owô", "", text)

    # 7. 감탄/놀람
    text = re.sub(r"\*_\*", "", text)
    text = re.sub(r"\+_\+", "", text)
    text = re.sub(r"°_°", "", text)
    text = re.sub(r"¤_¤", "", text)
    text = re.sub(r"\*o\*", "", text)
    text = re.sub(r"\*w\*", "", text)
    text = re.sub(r"\*W\*", "", text)
    text = re.sub(r"\*v\*", "", text)
    text = re.sub(r"\*V\*", "", text)
    text = re.sub(r"O_o'", "", text)
    text = re.sub(r"\+-\+", "", text)
    text = re.sub(r"\*\^\*", "", text)

    # 8. 부끄러운 표정
    text = re.sub(r"\^///\^", "", text)
    text = re.sub(r"O///O", "", text)
    text = re.sub(r"T///T", "", text)
    text = re.sub(r"°///°", "", text)
    text = re.sub(r">///<<", "", text)
    text = re.sub(r"\^\^'", "", text)

    # 9. 짜증/실망
    text = re.sub(r"-_-'", "", text)
    text = re.sub(r"_\"", "", text)
    text = re.sub(r">_>", "", text)
    text = re.sub(r"'_'", "", text)
    text = re.sub(r"-\.-'", "", text)
    text = re.sub(r"¬¬", "", text)
    text = re.sub(r"--'", "", text)
    text = re.sub(r"-\.-\"", "", text)
    text = re.sub(r"u_u\"", "", text)
    text = re.sub(r"u_u'", "", text)
    text = re.sub(r"-w-\"", "", text)

    # 10. 입막음/침묵
    text = re.sub(r"TxT", "", text)
    text = re.sub(r"°x°", "", text)
    text = re.sub(r"\*x\*", "", text)
    text = re.sub(r"'x'", "", text)
    text = re.sub(r"\"x\"", "", text)
    text = re.sub(r"OxO", "", text)
    text = re.sub(r"'-'", "", text)

    # 11. 혼란/당황
    text = re.sub(r"é~è", "", text)
    text = re.sub(r"\(°~°\)", "", text)
    text = re.sub(r"\(@o@\)", "", text)

    # 12. 우는 표정
    text = re.sub(r"Q__Q", "", text)
    text = re.sub(r"T_T", "", text)
    text = re.sub(r";_;", "", text)
    text = re.sub(r"ç_ç", "", text)
    text = re.sub(r"QQ", "", text)
    text = re.sub(r"T-T", "", text)
    text = re.sub(r"TT_TT", "", text)
    text = re.sub(r"P\.P", "", text)
    text = re.sub(r"TAT", "", text)
    text = re.sub(r"T\^T", "", text)
    text = re.sub(r"TTwTT", "", text)
    text = re.sub(r"ToT", "", text)
    text = re.sub(r"T__T", "", text)

    # 13. 귀여운 표정
    text = re.sub(r"\^w\^", "", text)
    text = re.sub(r"owo", "", text)
    text = re.sub(r"OwO", "", text)
    text = re.sub(r"\^\.\^", "", text)
    text = re.sub(r"0w0", "", text)
    text = re.sub(r"éwè", "", text)

    # 14. 술취함/어지러움
    text = re.sub(r"o_\^", "", text)

    # 15. 하품
    text = re.sub(r"=o=", "", text)
    text = re.sub(r"-o-", "", text)
    text = re.sub(r"-0-", "", text)
    text = re.sub(r"=0=", "", text)

    # 16. 잠자는 표정
    text = re.sub(r"\(µ_µ\)", "", text)

    # 17. 무관심/시치미
    text = re.sub(r"\('\.\)_\('\.\)", "", text)
    text = re.sub(r"\.w\.", "", text)
    text = re.sub(r"\.__\.", "", text)

    # 18. 침 흘리기
    text = re.sub(r"\*q\*", "", text)
    text = re.sub(r"\*Q\*", "", text)
    text = re.sub(r"°q°", "", text)
    text = re.sub(r"°Q°", "", text)
    text = re.sub(r"TqT", "", text)
    text = re.sub(r"TQT", "", text)
    text = re.sub(r"\*µ\*", "", text)

    # 19. 아픈/멍한 표정
    text = re.sub(r"@_@", "", text)
    text = re.sub(r"x_x", "", text)

    # 20. 화난 표정
    text = re.sub(r"è_é", "", text)
    text = re.sub(r"èé", "", text)
    text = re.sub(r"èOé", "", text)
    text = re.sub(r"`_´", "", text)
    text = re.sub(r"-_-\+", "", text)
    text = re.sub(r"-_-#", "", text)
    text = re.sub(r"\*w\*#", "", text)
    text = re.sub(r">\.\>", "", text)

    # 21. 안경
    text = re.sub(r"\[o\]_\[o\]", "", text)
    text = re.sub(r"\[\.\]_\[\.\]", "", text)
    text = re.sub(r"\[°\]_\[°\]", "", text)
    text = re.sub(r"\[0\]_\[0\]", "", text)

    # 22. 사악한 표정
    text = re.sub(r"-w-", "", text)
    text = re.sub(r"èwé", "", text)

    # 23. 좌절/짜증
    text = re.sub(r">_<", "", text)
    text = re.sub(r"><", "", text)
    text = re.sub(r"~_~", "", text)
    text = re.sub(r">\.<", "", text)

    # 24. 피곤한 표정
    text = re.sub(r"v\.v", "", text)
    text = re.sub(r"~_~°", "", text)

    # 25. 죽은 표정
    text = re.sub(r"XoX", "", text)

    # 26. 기쁜 표정
    text = re.sub(r"\\\\o/", "", text)
    text = re.sub(r"\\\\\(\\^o\\^\)/", "", text)
    text = re.sub(r"\)°\(", "", text)

    # 27. 돈 표정
    text = re.sub(r"\$_\$", "", text)

    # 28. 키스
    text = re.sub(r"\^\*\^", "", text)
    text = re.sub(r"\^3\^", "", text)
    text = re.sub(r"°3°", "", text)
    text = re.sub(r"\*3\*", "", text)
    text = re.sub(r"°\*°", "", text)

    # 29. 다이빙
    text = re.sub(r"O u O\|", "", text)

    # 30. 걱정/불안
    text = re.sub(r"éoè", "", text)
    text = re.sub(r"éè", "", text)

    # 31. 웃음
    text = re.sub(r"ovô", "", text)

    # 32. 악마
    text = re.sub(r"èvé", "", text)

    # 33. 수염
    text = re.sub(r"\^m\^", "", text)
    text = re.sub(r"omo", "", text)

    # 34. 음악 듣기
    text = re.sub(r"o\|\^-\^\|o", "", text)

    # 35. 공부
    text = re.sub(r"\?@-@\?", "", text)

    # 36. 더위
    text = re.sub(r":::\^\^:::", "", text)

    # 37. 응원
    text = re.sub(r"~\^o\^~", "", text)

    # 서양식 이모티콘 제거
    text = re.sub(r":-\)", "", text)
    text = re.sub(r":\)", "", text)
    text = re.sub(r"=\)", "", text)
    text = re.sub(r"\(:", "", text)
    text = re.sub(r"X\)", "", text)
    text = re.sub(r"x\)", "", text)
    text = re.sub(r"\(x", "", text)
    text = re.sub(r":-\(", "", text)
    text = re.sub(r":\(", "", text)
    text = re.sub(r"=\(", "", text)
    text = re.sub(r"\):", "", text)
    text = re.sub(r":-\|", "", text)
    text = re.sub(r":\|", "", text)
    text = re.sub(r"=\|", "", text)
    text = re.sub(r"\|:", "", text)
    text = re.sub(r":-D", "", text)
    text = re.sub(r":D", "", text)
    text = re.sub(r"=D", "", text)
    text = re.sub(r";-\)", "", text)
    text = re.sub(r";\)", "", text)
    text = re.sub(r"\(;", "", text)
    text = re.sub(r":-P", "", text)
    text = re.sub(r":-p", "", text)
    text = re.sub(r":P", "", text)
    text = re.sub(r":p", "", text)
    text = re.sub(r"=P", "", text)
    text = re.sub(r"=p", "", text)
    text = re.sub(r":-O", "", text)
    text = re.sub(r":-o", "", text)
    text = re.sub(r":O", "", text)
    text = re.sub(r":o", "", text)
    text = re.sub(r"=O", "", text)
    text = re.sub(r"=o", "", text)
    text = re.sub(r"=-O", "", text)
    text = re.sub(r"o:", "", text)
    text = re.sub(r"8-\)", "", text)
    text = re.sub(r"8\)", "", text)
    text = re.sub(r"8-O", "", text)
    text = re.sub(r"8O", "", text)
    text = re.sub(r"X-D", "", text)
    text = re.sub(r"XD", "", text)
    text = re.sub(r"x-D", "", text)
    text = re.sub(r"xD", "", text)
    text = re.sub(r":-\$", "", text)
    text = re.sub(r":\$", "", text)
    text = re.sub(r"=\$", "", text)
    text = re.sub(r"\$:", "", text)
    text = re.sub(r":x", "", text)
    text = re.sub(r":-/", "", text)
    text = re.sub(r":/", "", text)
    text = re.sub(r"=/", "", text)
    text = re.sub(r"/:", "", text)
    text = re.sub(r"<3", "", text)
    text = re.sub(r":-X", "", text)
    text = re.sub(r":-x", "", text)
    text = re.sub(r":X", "", text)
    text = re.sub(r":-#", "", text)
    text = re.sub(r":#", "", text)
    text = re.sub(r"=X", "", text)
    text = re.sub(r"=x", "", text)
    text = re.sub(r"=#", "", text)
    text = re.sub(r"x:", "", text)
    text = re.sub(r":-S", "", text)
    text = re.sub(r":S", "", text)
    text = re.sub(r":-s", "", text)
    text = re.sub(r":s", "", text)
    text = re.sub(r"=S", "", text)
    text = re.sub(r"=s", "", text)
    text = re.sub(r"s:", "", text)
    text = re.sub(r":-\*", "", text)

    # 여러 공백을 하나로 합치고 앞뒤 공백 제거
    text = re.sub(r'\s+', ' ', text).strip()

    return text

# ============================================================================
# Filter Classes (from nemo_curator/stages/text/filters/)
# ============================================================================
class BasePersonalFilter:
    """
    A base filter that applies a series of basic text preprocessing steps.
    Used for languages without a specific filter or as a default.
    """
    def __init__(self):
        self._name = "base_preproc"

    def apply(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        # Basic whitespace normalization
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @property
    def name(self) -> str:
        return self._name

class PersonalFilter:
    """
    A filter that applies a series of personal text preprocessing steps
    to a given text string. This is designed to be used as a modifier
    on a per-sentence basis.
    """
    def __init__(self):
        self._name = "personal_preproc_filter"

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

        # 카오모지 제거
        try:
            text = _remove_kaomoji(text)
        except Exception:
            pass

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

# ============================================================================
# Main Utility Functions
# ============================================================================
def custom_normalize_whitespace(text):
    """공백 정규화"""
    return re.sub(r'\s+', ' ', text).strip()

def calculate_scores(text, lang, config):
    scores = {}
    lang_config = {**config['defaults'], **config.get(lang, {})}
    
    words = text.split()
    scores['word_count'] = len(words)
    
    total_chars = len(re.sub(r'\s', '', text))
    if total_chars > 0 and 'lang_char_pattern' in lang_config:
        lang_chars = len(re.findall(lang_config['lang_char_pattern'], text))
        scores['lang_ratio'] = lang_chars / total_chars
    else:
        scores['lang_ratio'] = 0

    lines = text.split('\n')
    scores['repeated_lines_uniqueness_ratio'] = len(set(lines)) / len(lines) if lines else 0

    n = lang_config['ngram_n']
    ngrams = [tuple(words[i:i+n]) for i in range(len(words)-n+1)]
    scores['repeating_duplicate_ngram_ratio'] = 1 - (len(set(ngrams)) / len(ngrams)) if ngrams else 0
    
    if words:
        scores['symbol_to_word_ratio'] = len(re.findall(r'[^\w\s가-힣a-zA-Z]', text)) / len(words)
        scores['urls_ratio'] = len(re.findall(r'https?://\S+', text)) / len(words)
    else:
        scores['symbol_to_word_ratio'] = 0
        scores['urls_ratio'] = 0

    weights = config['quality_weights']
    scores['quality_score'] = (
        scores.get('lang_ratio', 0) * weights['lang_ratio'] +
        scores.get('repeated_lines_uniqueness_ratio', 0) * weights['repeated_lines'] +
        (1.0 - scores.get('repeating_duplicate_ngram_ratio', 0)) * weights['repeated_ngrams'] +
        (1.0 - scores.get('symbol_to_word_ratio', 0)) * weights['symbols'] +
        (1.0 - scores.get('urls_ratio', 0)) * weights['urls']
    )
    return scores

def preprocess_text(text, lang, preprocessors):
    """Helper to preprocess a single text string."""
    if not text or not text.strip():
        return ""
    
    # kss for Korean sentence splitting
    if lang == 'ko':
        sentences = kss.split_sentences(text)
    else:
        # Basic sentence splitting for other languages
        sentences = text.split('. ')

    # Apply personal data filter
    preprocessor = preprocessors.get(lang, BasePersonalFilter())
    if lang == "ko":
        preprocessor = PersonalFilter() # 별도 PersonalFilter 사용

    processed_sentences = [preprocessor.apply(s) for s in sentences]
    processed_sentences = [s for s in processed_sentences if s]

    if not processed_sentences:
        return ""

    # Join and normalize
    joined_text = " ".join(processed_sentences)
    return custom_normalize_whitespace(joined_text)

def run_filters_on_text(text, lang, config, scores):
    """Check if a processed text passes all filters."""
    lang_config = {**config['defaults'], **config.get(lang, {})}

    if not text or not text.strip():
        return False, "preprocessing_empty", "전처리 후 빈 텍스트"
    if 'min_words' in lang_config and not (lang_config['min_words'] <= scores['word_count'] <= lang_config['max_words']):
        return False, "word_count", f"단어 수: {scores['word_count']}"
    if 'lang_threshold' in lang_config and scores['lang_ratio'] < lang_config['lang_threshold']:
        return False, "language", f"{lang} 문자 비율: {scores['lang_ratio']:.3f}"
    if (1 - scores['repeated_lines_uniqueness_ratio']) > lang_config['max_repeated_line_fraction']:
        return False, "repeated_lines", f"반복 라인 비율: {1 - scores['repeated_lines_uniqueness_ratio']:.3f}"
    if scores['repeating_duplicate_ngram_ratio'] > lang_config['max_repeating_ngram_ratio']:
        return False, "repeated_ngrams", f"반복 N-gram 비율: {scores['repeating_duplicate_ngram_ratio']:.3f}"
    if scores['symbol_to_word_ratio'] > lang_config['max_symbol_to_word_ratio']:
        return False, "symbols", f"특수기호 비율: {scores['symbol_to_word_ratio']:.3f}"
    if scores['urls_ratio'] > lang_config['max_url_ratio']:
        return False, "urls", f"URL 비율: {scores['urls_ratio']:.3f}"
    if scores['quality_score'] < lang_config['min_quality_score']:
        return False, "quality_score", f"품질 점수: {scores['quality_score']:.3f}"
        
    return True, "passed", ""

