"""
A NeMo Curator stage to perform detailed linguistic analysis on each document
and add the features to the document.

This stage uses Stanza to perform dependency parsing and calculates various features
based on the parsed sentence structure, including lexical diversity, clause structure,
POS-based metrics, and dependency tree analysis.
"""

import stanza
import numpy as np
from scipy.stats import entropy
from collections import Counter
import torch
from nemo_curator.stages.stage import Stage

# PyTorch weights_only issue fix for Stanza compatibility
if hasattr(torch, 'serialization'):
    try:
        torch.serialization.add_safe_globals(['numpy.core.multiarray._reconstruct'])
    except:
        pass

# ============================================================================
# Feature Calculation Functions (from one_sentence.py)
# ============================================================================

# ========== 1. 기본 토큰 통계 ==========
def calculate_token_count(sent):
    return len(sent.words)

def calculate_avg_token_length(sent):
    tokens = [word.text for word in sent.words]
    if not tokens: return 0
    return round(np.mean([len(t) for t in tokens]), 2)

# ========== 2. 어휘 다양성 ==========
def calculate_lexical_diversity(sent):
    tokens = [word.text for word in sent.words]
    if len(tokens) == 0: return 0
    return round(len(set(tokens)) / len(tokens), 4)

# ========== 3. 절(Clause) 구조 분석 ==========
def calculate_main_clauses(sent):
    dep_list = [word.deprel for word in sent.words]
    return sum(1 for d in dep_list if d == "root")

def calculate_subordinate_clauses(sent):
    dep_list = [word.deprel for word in sent.words]
    subordinate_count = 0
    for dep in dep_list:
        if dep.startswith('acl') or dep.startswith('csubj') or dep in ['advcl', 'ccomp', 'xcomp', 'parataxis']:
            subordinate_count += 1
    return subordinate_count

def calculate_subordinate_ratio(sent):
    main_clauses = calculate_main_clauses(sent)
    subordinate_clauses = calculate_subordinate_clauses(sent)
    total_clauses = main_clauses + subordinate_clauses
    if total_clauses == 0: return 0
    return round(subordinate_clauses / total_clauses * 100, 2)

# ========== 4. 품사(POS) 기반 분석 ==========
def calculate_content_word_ratio(sent):
    pos_list = [word.upos for word in sent.words]
    token_count = len(pos_list)
    if token_count == 0: return 0
    content_pos = {'NOUN', 'PROPN', 'VERB', 'ADJ', 'ADV', 'NUM'}
    content_word_count = sum(1 for pos in pos_list if pos in content_pos)
    return round(content_word_count / token_count * 100, 2)

def calculate_noun_verb_ratio(sent):
    pos_counts = Counter(word.upos for word in sent.words)
    noun_count = pos_counts.get('NOUN', 0) + pos_counts.get('PROPN', 0)
    verb_count = pos_counts.get('VERB', 0)
    if verb_count == 0: return 0
    return round(noun_count / verb_count, 2)

def calculate_modifier_ratio(sent):
    pos_list = [word.upos for word in sent.words]
    token_count = len(pos_list)
    if token_count == 0: return 0
    pos_counts = Counter(pos_list)
    adj_count = pos_counts.get('ADJ', 0)
    adv_count = pos_counts.get('ADV', 0)
    return round((adj_count + adv_count) / token_count * 100, 2)

def calculate_pos_sequence_entropy(sent):
    pos_list = [word.upos for word in sent.words]
    if not pos_list: return 0
    pos_counts = Counter(pos_list)
    counts = np.array(list(pos_counts.values()))
    return round(entropy(counts / counts.sum(), base=2), 4)

# ========== 5. 구문 트리 깊이 분석 ==========
def calculate_dependency_depths(sent):
    word_dict = {w.id: w for w in sent.words}
    computed_depths = {}
    def get_depth(word_id):
        if word_id == 0: return 0
        if word_id in computed_depths: return computed_depths[word_id]
        word_obj = word_dict.get(word_id)
        if not word_obj:
            computed_depths[word_id] = 0
            return 0
        depth = get_depth(word_obj.head) + 1
        computed_depths[word_id] = depth
        return depth
    return [get_depth(word.id) for word in sent.words]

def calculate_dependency_depth_variance(sent):
    depths_list = calculate_dependency_depths(sent)
    if not depths_list: return 0
    return round(np.var(depths_list), 4)

# ========== 6. Dependency 거리 분석 ==========
def calculate_max_dependency_distance(sent):
    max_distance = 0
    for word in sent.words:
        if word.head > 0:
            dist = abs(word.id - word.head)
            if dist > max_distance:
                max_distance = dist
    return max_distance

# ========== 7. 트리 균형 점수 ==========
def calculate_tree_balance_score(sent):
    children_map = defaultdict(list)
    for word in sent.words:
        if word.head > 0:
            children_map[word.head].append(word)
    word_balances = []
    for word in sent.words:
        children = children_map.get(word.id, [])
        if not children:
            balance_w = 1.0
        else:
            left_deps = sum(1 for c in children if c.id < word.id)
            right_deps = sum(1 for c in children if c.id > word.id)
            balance_w = (min(left_deps, right_deps) + 1) / (max(left_deps, right_deps) + 1)
        word_balances.append(balance_w)
    if not word_balances: return 0
    return round(np.mean(word_balances), 4)

# ========== 8. 등위접속 분석 ==========
def calculate_coordination_ratio(sent):
    dep_list = [word.deprel for word in sent.words]
    if not dep_list: return 0
    coord_deps = {'conj', 'cc', 'appos', 'list', 'parataxis'}
    coord_count = sum(1 for dep in dep_list if dep in coord_deps)
    return round(coord_count / len(dep_list), 4)

# ========== 9. 구조 점수 (번역 품질) ==========
def calculate_structure_score(sent):
    main_clauses = calculate_main_clauses(sent)
    subord_ratio = calculate_subordinate_ratio(sent)
    has_main_clause = 1 if main_clauses > 0 else 0
    clause_balance = 1 - abs(subord_ratio - 30) / 100
    return round(has_main_clause * 50 + clause_balance * 50, 2)

# ========== 10. 장르/스타일 점수 ==========
def calculate_genre_score(sent):
    noun_verb_ratio = calculate_noun_verb_ratio(sent)
    modifier_ratio = calculate_modifier_ratio(sent)
    depth_variance = calculate_dependency_depth_variance(sent)
    avg_length = calculate_avg_token_length(sent)
    noun_score = max(0, min(30, (noun_verb_ratio - 0.5) / 2.5 * 30))
    modifier_score = (15 - modifier_ratio) / 15 * 15 if modifier_ratio < 15 else max(0, 25 - (modifier_ratio - 15) / 15 * 25)
    complexity_score = min(25, depth_variance / 10 * 25)
    length_score = max(0, min(20, (avg_length - 3) / 3 * 20))
    return round(noun_score + modifier_score + complexity_score + length_score, 2)

# ========== 11. 전체 Feature 계산 (통합 함수) ==========
def calculate_all_features(sent):
    return {
        'avg_token_length': calculate_avg_token_length(sent),
        'main_clauses': calculate_main_clauses(sent),
        'subordinate_clauses': calculate_subordinate_clauses(sent),
        'content_word_ratio': calculate_content_word_ratio(sent),
        'noun_verb_ratio': calculate_noun_verb_ratio(sent),
        'modifier_ratio': calculate_modifier_ratio(sent),
        'pos_sequence_entropy': calculate_pos_sequence_entropy(sent),
        'dependency_depth_variance': calculate_dependency_depth_variance(sent),
        'max_dependency_distance': calculate_max_dependency_distance(sent),
        'tree_balance_score': calculate_tree_balance_score(sent),
        'coordination_ratio': calculate_coordination_ratio(sent),
        'structure_score': calculate_structure_score(sent),
        'genre_score': calculate_genre_score(sent),
    }

# ============================================================================
# NeMo Curator Stage Class
# ============================================================================

class SentenceAnalysisStage(Stage):
    """
    A NeMo Curator stage to perform detailed linguistic analysis on documents.
    """
    def __init__(self):
        super().__init__()
        self.pipelines = {}
        self.lang_code_map = {"korean": "ko", "english": "en", "japanese": "ja", "chinese": "zh"}

    def _get_pipeline(self, lang_code):
        if lang_code not in self.pipelines:
            original_load = torch.load
            def patched_load(*args, **kwargs):
                kwargs['weights_only'] = False
                return original_load(*args, **kwargs)
            torch.load = patched_load
            try:
                self.pipelines[lang_code] = stanza.Pipeline(lang_code, processors='tokenize,pos,lemma,depparse', verbose=False, use_gpu=False)
            except Exception:
                stanza.download(lang_code, verbose=False)
                self.pipelines[lang_code] = stanza.Pipeline(lang_code, processors='tokenize,pos,lemma,depparse', verbose=False, use_gpu=False)
            torch.load = original_load
        return self.pipelines[lang_code]

    def _analyze_text(self, text, lang_name, features_field):
        if not text:
            return {}
        
        lang_code = self.lang_code_map.get(lang_name.lower(), "en")
        try:
            nlp = self._get_pipeline(lang_code)
            st_doc = nlp(text)
            if st_doc.sentences:
                return {features_field: calculate_all_features(st_doc.sentences[0])}
        except Exception as e:
            return {f"{features_field}_error": str(e)}
        return {}

    def _process_document(self, doc):
        """
        Analyzes 'input' and 'output' fields of a document if they exist.
        """
        # Analyze 'input' field
        input_analysis = self._analyze_text(doc.get("input", ""), doc.get("src", "en"), 'input_features')
        doc.update(input_analysis)
        
        # Analyze 'output' field
        output_analysis = self._analyze_text(doc.get("output", ""), doc.get("tgt", "en"), 'output_features')
        doc.update(output_analysis)

        return doc
