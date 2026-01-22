import re

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
    """Helper to preprocess a single text string. Preprocessing is disabled."""
    # The user requested to disable preprocessing. Return the original text.
    return text

def run_filters_on_text(text, lang, config, scores):
    """Check if a processed text passes all filters. Filtering is disabled."""
    # The user requested to disable filtering. Always pass.
    return True, "passed", ""