import re
import kss
import html
import unicodedata
from .remove_kaomoji import _remove_kaomoji


class BasePreprocessing:
    """
    A base filter that applies a series of comprehensive text preprocessing steps.
    Used for all languages as a default preprocessor.
    """
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
        
        # 연속된 구두점 정리 (!!!, ??? 등을 !, ? 로)
        text = re.sub(r'!{3,}', '!', text)
        text = re.sub(r'\?{3,}', '?', text)
        
        # 불필요한 공백 제거
        text = re.sub(r' \.(?!\d)', '.', text)  # 반점 뒤에 숫자가 있는 경우 공백을 없애지 않음
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
        
        # 기호 제거 (언어 중립적 기호들)
        patterns = {
            "도형 및 화살표": r'[※〓▪♦]',
            "음악 관련 기호": r'[♪♫♬♭♮♯]',
            "카드 및 게임": r'[♠♤♣♧★☆♥♡✪¤]',
            "특수 화살표": r'[ᐅ]',
            "하이픈류": r'[—]',
            # 포괄적 이모지 제거
            "이모지 - 얼굴 표정": r'[\U0001F600-\U0001F64F]',
            "이모지 - 기호 및 그림문자": r'[\U0001F300-\U0001F5FF]',
            "이모지 - 교통 및 지도": r'[\U0001F680-\U0001F6FF]',
            "이모지 - 추가 기호": r'[\U0001F900-\U0001F9FF]',
            "이모지 - 확장 기호": r'[\U0001FA70-\U0001FAFF]',
            "이모지 - 기타 기호": r'[\U00002600-\U000026FF]',
            "이모지 - Dingbats": r'[\U00002700-\U000027BF]',
            "이모지 - 피부색 modifier": r'[\U0001F3FB-\U0001F3FF]',
            "이모지 - 국기": r'[\U0001F1E0-\U0001F1FF]',
        }
        for pattern in patterns.values():
            text = re.sub(pattern, '', text)
        
        text = re.sub('┃', '|', text)
        
        # 카오모지 제거 (언어 중립적)
        try:
            text = _remove_kaomoji(text)
        except Exception:
            pass
        
        # 이상한 거 제거
        text = re.sub('[- ]$', '', text)
        text = re.sub('[, ]$', '', text)
        text = re.sub('[ , ]$', '', text)
        text = re.sub(r'[\[\] ]$', '', text)
        
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

def preprocess_text(text, lang, preprocessors=None):
    """
    Helper to preprocess a single text string using a dictionary of preprocessors.
    If preprocessors is not provided, uses BasePreprocessing as default.
    """
    if not text or not text.strip():
        return ""
    
    # If no preprocessors dictionary is provided, use BasePreprocessing
    if preprocessors is None:
        preprocessor = BasePreprocessing()
    else:
        preprocessor = preprocessors.get(lang, BasePreprocessing())
    
    # For Korean, use kss for sentence splitting.
    # For other languages, use a simple split as a fallback.
    if lang == 'ko':
        try:
            sentences = kss.split_sentences(text)
        except Exception:
            # In case of an error with kss, use a simple split.
            sentences = re.split(r'(?<=[.!?])\s+', text)
    else:
        sentences = re.split(r'(?<=[.!?])\s+', text)
    
    # Apply the selected preprocessor to each sentence.
    processed_sentences = [preprocessor.apply(s) for s in sentences]
    
    # Filter out any empty sentences that might result from preprocessing.
    processed_sentences = [s for s in processed_sentences if s]
    
    if not processed_sentences:
        return ""
    
    # Join the processed sentences and normalize whitespace.
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