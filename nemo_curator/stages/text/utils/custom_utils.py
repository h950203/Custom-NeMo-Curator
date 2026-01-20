import re
import kss
from nemo_curator.stages.text.filters.personal_filter import PersonalFilter
from nemo_curator.stages.text.filters.base_personal_filter import BasePersonalFilter

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
