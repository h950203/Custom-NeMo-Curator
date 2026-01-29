# -*- coding: utf-8 -*-
# kiwipiepy                0.22.2

from kiwipiepy import Kiwi
import re


def sentence_completeness_score_v3(text: str, kiwi_instance: Kiwi) -> int:
    """
    문장의 완결성 점수를 계산 (0-100점) - v3
    
    평가 기준:
    1. 문장 시작 (15점)
    2. 주어 존재 (10점)
    3. 서술어 존재 및 품질 (35점)
    4. 문장 종결 표현 (30점) - [v3: 실질적 마지막 단어 검사 개선]
    5. 구조적 완성도 (10점)
    6. 구문 구조 일관성 (괄호 짝 검사)
    7. [NEW v3] 종결어미 필수 검사
    
    Args:
        text: 검사할 문장
        kiwi_instance: Kiwi 객체
    
    Returns:
        int: 완결성 점수 (0-100)
    """
    # 입력 문자열의 양쪽 공백을 모두 제거
    text = text.strip()
    if not text:
        return 0

    # 형태소 분석
    try:
        analyzed = kiwi_instance.analyze(text)
    except:
        try:
            analyzed = kiwi_instance.analyze(text, split_complex=False)
        except:
            return 0
    
    if not analyzed:
        return 0

    # kiwi가 문장을 분리하더라도 모든 토큰을 하나로 합쳐서 전체로 분석
    all_tokens = []
    for sentence_analysis in analyzed:
        if sentence_analysis and sentence_analysis[0]:
            all_tokens.extend(sentence_analysis[0])
    
    if not all_tokens:
        return 0

    tokens = all_tokens
    pos_tags = [token.tag for token in tokens]
    
    total_score = 0
    
    # ===== 1. 문장 시작 검사 (15점) =====
    first_token = tokens[0]
    invalid_start_tags = {'JC', 'JX', 'EC', 'ETM', 'JKB', 'JKO', 'EP', 'EF'}
    conjunctions = ['와', '과', '하고', '이랑', '랑', '며', '나', '및', '또는', '그리고', '그러나', '하지만']
    number_patterns = ['(', ')', '[', ']', '①', '②', '③', '1)', '2)', '3)', 'ⅰ', 'ⅱ', 'ⅲ']
    
    start_score = 0
    if first_token.tag in invalid_start_tags:
        start_score = 0
    elif first_token.form in conjunctions:
        start_score = 0
    elif first_token.form in number_patterns or first_token.form.isdigit():
        start_score = 3
    elif first_token.tag in {'SF', 'SP', 'SS', 'SE'}:
        start_score = 5
    elif first_token.tag == 'SN':
        start_score = 1
    else:
        start_score = 15
    
    total_score += start_score
    
    # ===== 2. 주어 존재 검사 (10점) =====
    has_subject_marker = any(tag == 'JKS' for tag in pos_tags)
    has_topic_marker = any(tag == 'JX' for tag in pos_tags)
    has_noun = any(tag.startswith('N') for tag in pos_tags)
    
    if has_subject_marker:
        total_score += 10
    elif has_topic_marker:
        total_score += 8
    elif has_noun:
        total_score += 5
    else:
        total_score += 0
    
    # ===== 3. 서술어 존재 및 품질 검사 (35점) =====
    verbs = [token for token in tokens if token.tag.startswith('VV')]
    adjectives = [token for token in tokens if token.tag.startswith('VA')]
    copulas = [i for i, token in enumerate(tokens) if token.tag == 'VCP']
    
    has_verb = len(verbs) > 0
    has_adjective = len(adjectives) > 0
    has_copula_predicate = False
    if copulas:
        for idx in copulas:
            if idx > 0 and tokens[idx-1].tag.startswith(('N', 'SN')):
                has_copula_predicate = True
                break

    has_aux = any(tag == 'VX' for tag in pos_tags)
    has_ec = any(tag == 'EC' for tag in pos_tags)
    
    predicate_score = 0
    if has_copula_predicate:
        predicate_score = 33
    elif has_verb and has_adjective:
        predicate_score = 35
    elif has_verb and has_aux:
        predicate_score = 32
    elif has_verb and has_ec:
        predicate_score = 30
    elif has_verb:
        predicate_score = 28
    elif has_adjective and has_aux:
        predicate_score = 25
    elif has_adjective:
        predicate_score = 22
    elif any(tag.startswith('V') for tag in pos_tags):
        predicate_score = 15

    total_score += predicate_score
    
    # ===== [NEW v3] 실질적 마지막 토큰 찾기 =====
    # 마침표, 쉼표 등 기호를 제외한 실제 마지막 단어
    last_meaningful_token = None
    symbol_tags = {'SF', 'SP', 'SS', 'SE', 'SW'}  # 기호 태그들
    
    for token in reversed(tokens):
        if token.tag not in symbol_tags:
            last_meaningful_token = token
            break
    
    # 기호만 있는 경우
    if last_meaningful_token is None:
        return min(10, total_score)
    
    # ===== 4. 문장 종결 표현 검사 (30점) - v3 대폭 개선 =====
    has_ef = any(token.tag == 'EF' for token in tokens)
    has_sf = any(token.tag == 'SF' for token in tokens)
    
    # 명백히 불완전한 종결 단어 목록
    incomplete_ending_words = {'또는', '및', '혹은', '그리고', '하지만', '그러나'}
    
    # [v3 핵심] 조사로 끝나는 경우 (가장 흔한 불완전 패턴)
    particle_tags = {'JKB', 'JKO', 'JKG', 'JKC', 'JKV', 'JKQ', 'JKS', 'JX', 'JC'}
    
    # 연결어미/관형사형 전성어미로 끝나는 경우
    connecting_tags = {'EC', 'ETM'}
    
    # 동사/형용사 어간으로 끝나는 경우
    stem_tags = {'VV', 'VA'}
    
    ending_score = 0
    
    # [v3] 우선순위별 검사
    if last_meaningful_token.form in incomplete_ending_words:
        ending_score = -25  # 명백히 불완전한 단어
    elif last_meaningful_token.tag in particle_tags:
        # 조사로 끝나는 경우 - 강력한 감점
        ending_score = -30
    elif last_meaningful_token.tag in connecting_tags:
        # 연결어미/관형사형으로 끝나는 경우
        ending_score = -25
        predicate_score = max(0, predicate_score - 20)  # 서술어 점수도 감점
    elif last_meaningful_token.tag in stem_tags:
        # 동사/형용사 어간으로 끝나는 경우
        ending_score = -25
    elif last_meaningful_token.tag == 'SN' or last_meaningful_token.form.isdigit():
        ending_score = -20  # 숫자로 종결
    elif last_meaningful_token.form in ['(', ')', '[', ']', '①', '②', '③']:
        ending_score = -20  # 괄호/기호로 종결
    elif has_ef and has_sf:
        # 종결어미 + 마침표 (가장 완전한 형태)
        ef_positions = [i for i, token in enumerate(tokens) if token.tag == 'EF']
        if ef_positions and ef_positions[-1] >= len(tokens) - 3:
            ending_score = 30
        else:
            ending_score = 25
    elif has_ef:
        # 종결어미만 있는 경우
        ef_positions = [i for i, token in enumerate(tokens) if token.tag == 'EF']
        if ef_positions and ef_positions[-1] >= len(tokens) - 2:
            ending_score = 25
        else:
            ending_score = 18
    elif has_sf and last_meaningful_token.tag.startswith('N'):
        # 명사 + 마침표 (불완전하지만 어느정도 형태)
        ending_score = 10
    elif last_meaningful_token.tag.startswith('N'):
        # 명사로 끝남 (마침표도 없음)
        ending_score = 5
    else:
        ending_score = 3
    
    total_score += ending_score
    
    # ===== 5. 구조적 완성도 검사 (10점) =====
    token_count = len(tokens)
    structure_score = 0
    
    if token_count >= 5:
        structure_score = 10
    elif token_count >= 3:
        structure_score = 7
    elif token_count >= 2:
        structure_score = 4
    
    # 감점 1: 특수문자만 있는 경우
    non_symbol_tokens = [t for t in tokens if not t.tag.startswith('S')]
    if len(non_symbol_tokens) == 0:
        total_score = min(total_score, 10)
    
    # 감점 2: 괄호/번호가 과도하게 많은 경우
    bracket_count = sum(1 for t in tokens if t.form in ['(', ')', '[', ']', '①', '②', '③', '④', '⑤'])
    number_count = sum(1 for t in tokens if t.tag == 'SN' or t.form.isdigit())
    if bracket_count >= 3 or number_count >= 3:
        structure_score = max(0, structure_score - 5)
        
    # 감점 3: 연속된 특수 부호 검사
    if re.search(r'(\.|\?|!|,){3,}', text):
        structure_score = max(0, structure_score - 5)

    total_score += structure_score

    # ===== 6. 구문 구조 일관성 (괄호 짝) =====
    open_brackets = "([{"
    close_brackets = ")]}"
    stack = []
    has_mismatch = False
    
    for char in text:
        if char in open_brackets:
            stack.append(char)
        elif char in close_brackets:
            pos = close_brackets.index(char)
            if len(stack) > 0 and open_brackets[pos] == stack[-1]:
                stack.pop()
            else:
                has_mismatch = True
                break
    
    if len(stack) > 0:
        has_mismatch = True

    if has_mismatch:
        total_score = 0

    # ===== 7. [NEW v3] 종결어미 필수 검사 =====
    # 종결어미(EF)가 없는 문장은 완결성이 낮으므로 점수 상한 설정
    if not has_ef:
        # 단, 명사+이다 구문은 예외 처리
        if not has_copula_predicate:
            total_score = min(total_score, 55)  # 최대 55점까지만

    return max(0, min(100, total_score))  # 0-100 사이로 제한

