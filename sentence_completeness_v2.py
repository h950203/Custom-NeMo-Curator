# -*- coding: utf-8 -*-
"""
sentence_completeness_thres70.py 의 코드를 기반으로 보완된
문장 완결성 점수 계산기 v2

사용법:
    from kiwipiepy import Kiwi
    from sentence_completeness_v2 import sentence_completeness_score_v2
    
    kiwi = Kiwi()
    score = sentence_completeness_score_v2("문장을 입력하세요.", kiwi)
    print(f"완결성 점수: {score}/100")
"""

from kiwipiepy import Kiwi
import re


def sentence_completeness_score_v2(text: str, kiwi_instance: Kiwi) -> int:
    print("\n[DEBUG] Running sentence_completeness_v2 - FINAL VERSION CHECK")
    """
    문장의 완결성 점수를 계산 (0-100점) - v2
    
    평가 기준:
    1. 문장 시작 (15점)
    2. 주어 존재 (10점)
    3. 서술어 존재 및 품질 (35점) - (v2: 서술격 조사 처리 개선)
    4. 문장 종결 표현 (30점)
    5. 구조적 완성도 (10점) - (v2: 연속 특수부호 감점 추가)
    6. [NEW] 구문 구조 일관성 (괄호 짝 검사) 
    
    Args:
        text: 검사할 문장
        kiwi_instance: Kiwi 객체
    
    Returns:
        int: 완결성 점수 (0-100)
    """
    # [FINAL DEBUG] 입력 문자열의 양쪽 공백을 모두 제거
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

    # [FINAL FIX] kiwi가 문장을 분리하더라도 모든 토큰을 하나로 합쳐서 전체로 분석
    all_tokens = []
    for sentence_analysis in analyzed:
        if sentence_analysis and sentence_analysis[0]:
            all_tokens.extend(sentence_analysis[0])
    
    if not all_tokens:
        return 0

    tokens = all_tokens
    pos_tags = [token.tag for token in tokens]
    
    total_score = 0
    
    # ===== 1. 문장 시작 검사 (15점) - 기존 로직 유지 =====
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
    
    # ===== 2. 주어 존재 검사 (10점) - 기존 로직 유지 =====
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
    
    # ===== 3. 서술어 존재 및 품질 검사 (35점) - v2.2 개선 =====
    verbs = [token for token in tokens if token.tag.startswith('VV')]
    adjectives = [token for token in tokens if token.tag.startswith('VA')]
    copulas = [i for i, token in enumerate(tokens) if token.tag == 'VCP'] # 서술격 조사 '이다'
    
    has_verb = len(verbs) > 0
    has_adjective = len(adjectives) > 0
    has_copula_predicate = False
    if copulas:
        for idx in copulas:
            # '이다' 앞에 명사/대명사/수사가 있는지 확인
            if idx > 0 and tokens[idx-1].tag.startswith(('N', 'SN')):
                has_copula_predicate = True
                break

    has_aux = any(tag == 'VX' for tag in pos_tags)
    has_ec = any(tag == 'EC' for tag in pos_tags)
    
    predicate_score = 0
    if has_copula_predicate:
        predicate_score = 33 # 명사+이다/아니다 형태의 완결된 서술어
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

    # [NEW v2.2] 서술어가 있어도, 문장이 연결형으로 끝나면 서술어 점수 감점
    last_token = tokens[-1]
    predicate_demanding_tags = {'EC', 'JKB'} # MAJ/MAG 제외 (아래에서 단어 직접 처리)
    if predicate_score > 0 and last_token.tag in predicate_demanding_tags:
        predicate_score = max(0, predicate_score - 20)
    
    total_score += predicate_score
    
    # ===== 4. 문장 종결 표현 검사 (30점) - v2.4 강화 =====
    has_ef = any(token.tag == 'EF' for token in tokens)
    has_sf = any(token.tag == 'SF' for token in tokens)
    last_token = tokens[-1]
    
    # [NEW v2.4] 명백히 불완전한 종결 단어 목록
    incomplete_ending_words = {'또는', '및', '혹은'}

    # 불완전한 종결을 나타내는 명백한 품사 태그 목록
    invalid_ending_tags = {
        'JKB', 'JKO', 'JKG', 'JKC', 'JKV', 'JKQ', 'JX', 'JC', # 각종 조사
        'EC',  # 연결 어미
        'ETM', # 관형사형 전성 어미
        'VV',  # 동사 어간
        'VA',  # 형용사 어간
    }
    
    ending_score = 0
    # 명백히 불완전한 종결에 대해서는 0점이 아닌 감점(-20)을 적용
    if last_token.form in incomplete_ending_words:
        ending_score = -20 # 명백히 불완전한 단어로 종결
    elif last_token.tag in invalid_ending_tags:
        ending_score = -20 # 명백히 불완전한 품사로 종결
    elif last_token.tag == 'SN' or last_token.form.isdigit():
        ending_score = -20  # 숫자로 종결
    elif last_token.form in ['(', ')', '[', ']', '①', '②', '③']:
        ending_score = -20  # 괄호/기호로 종결
    elif has_ef and has_sf:
        ef_positions = [i for i, token in enumerate(tokens) if token.tag == 'EF']
        if ef_positions and ef_positions[-1] >= len(tokens) - 3:
            ending_score = 30
        else:
            ending_score = 25
    elif has_ef:
        ef_positions = [i for i, token in enumerate(tokens) if token.tag == 'EF']
        if ef_positions and ef_positions[-1] >= len(tokens) - 2:
            ending_score = 23
        else:
            ending_score = 15
    elif has_sf and last_token.tag == 'SF':
        if any(token.tag.startswith('N') for token in tokens[-3:]):
            ending_score = 15
        else:
            ending_score = 12
    elif last_token.tag.startswith('N'):
        ending_score = 8
    else:
        ending_score = 5
    
    total_score += ending_score
    
    # ===== 5. 구조적 완성도 검사 (10점) - v2 개선 =====
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
        
    # [NEW v2] 감점 3: 연속된 특수 부호 검사
    if re.search(r'(\.|\?|!|,){3,}', text):
        structure_score = max(0, structure_score - 5)

    total_score += structure_score

    # ===== 6. [NEW v2] 구문 구조 일관성 (괄호 짝) - v2.1 강화 =====
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
                # 짝이 안 맞는 닫는 괄호 발견
                has_mismatch = True
                break
    
    # 짝이 안 맞는 여는 괄호가 남아있는 경우
    if len(stack) > 0:
        has_mismatch = True

    # 괄호 짝이 맞지 않으면 최종 점수를 0점으로 처리
    if has_mismatch:
        total_score = 0

    return min(100, total_score) # 최종 점수는 100점을 넘지 않도록


if __name__ == "__main__":
    kiwi = Kiwi()
    
    # v1과 동일한 테스트 문장
    test_sentences = [
        # === 완전한 문장들 (80점 이상 예상) ===
        "국제노동기구는 이러한 신청을 환영합니다.",
        "이 회의는 정부, 사업주 및 근로자 그룹을 대표하는 전문가들로 구성되었다.",
        "권한있는 기관은 선박건조 및 수리시설의 산업안전보건에 대한 법령을 제정·유지·관리해야 한다.",
        "그 대표는 교육과 훈련 프로그램의 효과성을 검토해야 한다.",
        "이것은 매우 중요한 회의이다.", # v2 개선 테스트 (서술격 조사)
        
        # === 불완전한 문장들 (50점 미만 예상) ===
        "와 그 대표는 교육과 훈련 프로그램의 효과성을 검토해야 한다.",
        "(1) 건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준 (2) 물질의 유해성 여부 결정을 위해 필요한 정보의 적절성 평가 시스템 및 기준 4",
        "xiv",
        "숙소 ············································································································································································ 114",
        "참고문헌 ························································································································································· 116",
        "소 개 1.",
        "이것은 중요한 회의이지만 (결론은 나지 않았다", # v2 개선 테스트 (괄호 불일치)
        "이럴수가!!!!!!!!", # v2 개선 테스트 (연속 특수부호)

        # === 중간 수준 (50-70점 예상) ===
        "권한있는 기관: 법적 구속력이 있는 법령, 명령 또는 지시를 공표 및 이행할 수 있는 부처, 정부부서 또는", # <--- 사용자 테스트 문장으로 변경
        "선박건조 및 수리에 사용되는 물질은 이 요건에 따라 표시 및 표기되어야 한다.",
        "건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준",
    ]
    
    print("=" * 70)
    print("문장 완결성 점수 계산 테스트 (v2)")
    print("=" * 70)
    
    for sentence in test_sentences:
        score = sentence_completeness_score_v2(sentence, kiwi)
    
        print(f"\n문장: {sentence[:60]}")
        print(f"스코어: {score}")
