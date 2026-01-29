# -*- coding: utf-8 -*-
"""
sentence_completeness_v3.py - 조사/연결어미로 끝나는 불완전 문장 감지 개선

주요 개선사항:
- 마침표 등 기호를 제외한 실질적 마지막 단어 검사
- 조사로 끝나는 경우 강력한 감점
- 종결어미 없으면 점수 상한선 설정
- 연결어미/관형사형으로 끝나는 경우 감점 강화

사용법:
    from kiwipiepy import Kiwi
    from sentence_completeness_v3 import sentence_completeness_score_v3
    
    kiwi = Kiwi()
    score = sentence_completeness_score_v3("문장을 입력하세요.", kiwi)
    print(f"완결성 점수: {score}/100")
"""

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


if __name__ == "__main__":
    kiwi = Kiwi()
    
    # 문제가 되었던 문장들 + 기존 테스트 문장들
    test_sentences = [
    "(1) 건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준 (2) 물질의 유해성 여부 결정을 위해 필요한 정보의 적절성 평가 시스템 및 기준 4",
    "(3) 물질의 표시와 표기를 위한 요건.",
    "선박건조 및 수리에 사용되는 물질은 이 요건에 따라 표시 및 표기되어야 한다.",
    "(4) 사업주에게 제공되는 물질안전보건자료에 포함되는 정보에 대한 기준 (5) 선박건조 및 수리에 사용되는 구조물, 시설, 기계류, 장비, 프로세스, 작업과 관련하여 안전 유해요인과 적절한 위험관리 조치를 식별하는 시스템 및 기준 4.",
    "권한있는 기관은 이러한 기준과 요건을 정하기 위해 필요한 규칙을 마련해야 하나, 기관 자체가 반드시 기술적 과업이나 실험실 테스트를 수행할 필요는 없다.",
    "권한있는 기관은 안전보건을 위해 필요한 경우 다음 조치를 취해야 한다.",
    "(1) 유해한 특정 관행, 프로세스나 물질의 사용을 금지 또는 제한하거나, (2) 제한된 관행, 프로세스 및 물질을 사용하기 전에 사전통지를 하고 허가를 받을 것을 요구하거나 또는 (3) 안전보건상의 이유로 특정 프로세스나 물질의 사용이 금지된 또는 국가법령에 규정된 조건하에서만 사용이 허가된 근로자의 범주를 명시한다.",
    "권한있는 기관은 충분하고 적절한 검사 시스템을 통해 앞에서 언급한 정책과 관련된 국가법령의 이행을 보장해야 한다.",
    "이 이행시스템은 사업주 및 근로자 대표가 참여하는 협의과정을 통해 수립되어야 하며 정책 관련 국가법령 위반에 대한 시정조치와 적절한 처벌을 제공해야 한다.",
    "국가법령이나 권한있는 기관은 선박건조 및 수리시설의 안전보건 증진을 위해 사업주와 근로자 간의 조직적 협력을 보장하기 위한 조치를 규정해야 한다.",
    "해당 조치에는 다음 사항이 포함되어야 한다.",
    "(1) 사업주와 근로자를 대표하여 규정된 권한과 임무를 수행하는 안전보건위원회의 설립 (2) 규정된 권한과 임무를 수행하는 근로자 안전보건 대표의 선출이나 임명 (3) 안전보건 증진을 위한 적절한 자격과 경험을 갖춘 인물의 사업주에 의한 임명 (4) 안전보건 대표와 안전보건위원회 위원들에 대한 교육 8.",
    "권한있는 기관은 사업주, 근로자 및 그 대표가 정책상의 법적 의무를 준수하도록 지침을 마련하고, 산업안전보건에 대한 각자의 책임, 임무 및 권리 이행을 위한 지원을 제공해야 한다.",
    "권한있는 기관은 선박건조 및 수리업에서 발생하는 업무상 사고, 업무상 질병 및 위험사고에 대해 사업주가 성별로 분류하여 기록·통보하는 시스템을 수립 및 적용하고 정기적으로 검토해야 한다.",
    "감독기관 1.",
    "감독기관은 권한있는 기관이 임명하며, 국가법령에 규정된 바에 따라 다음 사항을 수행한다.",
    "(1) 선박건조 및 수리시설과 관련된 모든 법령을 이행한다.",
    "(2) 사업주와 근로자 대표의 입회 하에 정기적으로 점검을 실시하고 모든 관련 법령의 이행을 모니터한다.",
    "(3) 사업주, 근로자 및 그 대표의 산업안전보건에 대한 책임, 임무 및 권리 이행을 지원한다.",
    "(4) 비교가능한 국내외 선박건조 및 수리시설의 산업안전보건 요건과 성과를 모니터하여 추가적인 안전조치의 개발과 개선을 위한 피드백을 제공한다.",
    "(5) 사업주와 근로자를 대표하는 공인된 단체와 협력하여 국가와 기업 차원에서 채택할 수 있는 안전 규칙과 조치를 수립하고 현행화하는데 참여한다.",
    "감독관은 국가법령에 규정된 바에 따라 다음의 역할을 수행해야 한다.",
    "(1) 선박건조 및 수리업과 관련된 모든 근로자의 산업안전보건 문제를 다루고 지원 및 자문을 제공할 수 있는 역량을 갖춘다.",
    "(2) 사망과 중상사고, 위험사고 및 질병을 조사할 수 있는 권한을 가진다.",
    "(3) 사업주, 관련 근로자와 그 대표 및 안전보건위원회에 점검 결과와 필요한 시정조치를 통보한다.",
    "(4) 인명이나 보건에 즉각적이거나 중대한 위험을 미치는 상황에서 근로자를 대피시킬 권한을 가진다.",
    "(5) 산업안전보건관리시스템이나 해당 요소가 구비되어 있으며 적절하고 효과적인지 여부를 정기적으로 결정한다.",
    "(6) 안전보건을 위해 시정조치가 취해지기 전까지 선박건조 및 수리작업을 중지 또는 제한할 수 있다.",
    "(7) 모든 근로자의 안전보건 교육 및 훈련 기록을 열람할 수 있다.",
    "감독관의 권한, 권리, 절차 및 책임은 모든 관련 당사자에게 전달되어야 한다.",
    "사업주 1. 사업주는 작업장에 있는 모든 근로자의 안전보건을 조직화하고 보호 및 촉진할 의무가 있다.",
    "사업주는 국내외적으로 공인된 적절한 규범, 규약 및 지침을 포함하여 권한있는 기관이 규정, 승인 또는 인정한 바에 따라 선박건조 및 수리업의 안전보건 유해·위험요인에 관한 조치를 준수해야 한다.",
    "2. 사업주는 작업장, 공장, 장비, 도구, 기계류를 제공·유지관리 해야 하며, 선박건조 및 수리업의 유해요인과 위험을 제거하거나 제거가 불가능할 경우 관리할 수 있도록 업무를 설계하고 국가 법령을 준수해야 한다.",
    "사업주는 산업안전보건 분야 일반정책의 일환으로 각 프로그램, 절차 및 동 절차에 따라 수행되는 다양한 책임을 서면으로 작성해야 한다.",
    "이 정보는 구두, 서면 또는 근로자의 능력에 상응하는 기타 적절한 수단을 통해 명확하게 전달되어야 한다.",
    "4. 사업주는 근로자 및 그 대표와 협의하여 다음을 이행해야 한다.",
    ]
    
    print("=" * 70)
    print("문장 완결성 점수 계산 테스트 (v3 - 개선버전)")
    print("=" * 70)
    
    # 점수별로 문장 분류를 위한 리스트
    below_60 = []  # 60점 미만
    above_60 = []  # 60점 이상
    
    for sentence in test_sentences:
        score = sentence_completeness_score_v3(sentence, kiwi)
    
        print(f"\n문장: {sentence[:60]}")
        print(f"스코어: {score}")
        
        # 점수에 따라 분류
        if score < 60:
            below_60.append((sentence, score))
        else:
            above_60.append((sentence, score))
    
    # 60점 미만 문장을 txt 파일로 저장
    with open("sentences_below_60_v3.txt", "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("완결성 점수 60점 미만 문장 목록 (v3)\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"총 {len(below_60)}개 문장\n\n")
        
        for idx, (sentence, score) in enumerate(below_60, 1):
            f.write(f"{idx}. [점수: {score}점]\n")
            f.write(f"   {sentence}\n\n")
    
    # 60점 이상 문장을 txt 파일로 저장
    with open("sentences_above_60_v3.txt", "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("완결성 점수 60점 이상 문장 목록 (v3)\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"총 {len(above_60)}개 문장\n\n")
        
        for idx, (sentence, score) in enumerate(above_60, 1):
            f.write(f"{idx}. [점수: {score}점]\n")
            f.write(f"   {sentence}\n\n")
    
    print("\n" + "=" * 70)
    print("파일 저장 완료!")
    print(f"- sentences_below_60.txt: {len(below_60)}개 문장")
    print(f"- sentences_above_60.txt: {len(above_60)}개 문장")
    print("=" * 70)