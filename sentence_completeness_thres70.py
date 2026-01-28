# -*- coding: utf-8 -*-
"""
문장 완결성 점수 계산기 (상세 평가 버전)

사용법:
    from kiwipiepy import Kiwi
    from sentence_completeness_simple import get_completeness_score
    
    kiwi = Kiwi()
    score = get_completeness_score("문장을 입력하세요.", kiwi)
    print(f"완결성 점수: {score}/100")
"""

from kiwipiepy import Kiwi


def sentence_completeness_score(text: str, kiwi_instance: Kiwi) -> int:
    """
    문장의 완결성 점수를 계산 (0-100점)
    
    평가 기준:
    1. 문장 시작 (15점)
    2. 주어 존재 (10점)
    3. 서술어 존재 및 품질 (35점)
    4. 문장 종결 표현 (30점)
    5. 구조적 완성도 (10점)
    
    Args:
        text: 검사할 문장
        kiwi_instance: Kiwi 객체
    
    Returns:
        int: 완결성 점수 (0-100)
    """
    
    # 형태소 분석
    try:
        analyzed = kiwi_instance.analyze(text)
    except:
        try:
            analyzed = kiwi_instance.analyze(text, split_complex=False)
        except:
            return 0
    
    if not analyzed or not analyzed[0] or not analyzed[0][0]:
        return 0
    
    tokens = analyzed[0][0]
    pos_tags = [token.tag for token in tokens]
    
    total_score = 0
    
    # ===== 1. 문장 시작 검사 (15점) =====
    first_token = tokens[0]
    
    # 명백히 부적절한 시작
    invalid_start_tags = {'JC', 'JX', 'EC', 'ETM', 'JKB', 'JKO', 'EP', 'EF'}
    
    # 접속사로 시작 (와, 과, 하고, 및, 또는 등)
    conjunctions = ['와', '과', '하고', '이랑', '랑', '며', '나', '및', '또는', '그리고', '그러나', '하지만']
    
    # 괄호/번호로 시작하는 패턴
    number_patterns = ['(', ')', '[', ']', '①', '②', '③', '1)', '2)', '3)', 'ⅰ', 'ⅱ', 'ⅲ']
    
    start_score = 0
    
    if first_token.tag in invalid_start_tags:
        start_score = 0  # 명백히 잘못된 시작
    elif first_token.form in conjunctions:
        start_score = 0  # 접속사로 시작 (불완전)
    elif first_token.form in number_patterns or first_token.form.isdigit():
        start_score = 3  # 번호로 시작 (목록 항목)
    elif first_token.tag in {'SF', 'SP', 'SS', 'SE'}:
        start_score = 5  # 문장부호로 시작
    elif first_token.tag == 'SN':  # 숫자
        start_score = 1  # 숫자로 시작
    else:
        start_score = 15  # 정상적인 시작
    
    total_score += start_score
    
    # ===== 2. 주어 존재 검사 (10점) =====
    # 주격조사(JKS) 또는 명사(NNG, NNP, NNB) + 보조사(JX) 패턴
    has_subject_marker = any(tag == 'JKS' for tag in pos_tags)
    has_topic_marker = any(tag == 'JX' for tag in pos_tags)  # 보조사 (은/는)
    
    # 명사 존재 여부
    has_noun = any(tag.startswith('N') for tag in pos_tags)
    
    if has_subject_marker:
        total_score += 10  # 명확한 주어
    elif has_topic_marker:
        total_score += 8   # 주제어
    elif has_noun:
        total_score += 5   # 명사는 있지만 조사 없음
    else:
        total_score += 0
    
    # ===== 3. 서술어 존재 및 품질 검사 (35점) =====
    verbs = [token for token in tokens if token.tag.startswith('VV')]  # 동사
    adjectives = [token for token in tokens if token.tag.startswith('VA')]  # 형용사
    
    has_verb = len(verbs) > 0
    has_adjective = len(adjectives) > 0
    
    # 보조용언 확인
    has_aux = any(tag == 'VX' for tag in pos_tags)
    
    # 연결어미 확인 (복문 구조)
    has_ec = any(tag == 'EC' for tag in pos_tags)
    
    if has_verb and has_adjective:
        total_score += 35  # 동사와 형용사 모두 존재 (복잡한 문장)
    elif has_verb and has_aux:
        total_score += 32  # 동사 + 보조용언 (완전한 서술)
    elif has_verb and has_ec:
        total_score += 30  # 동사 + 연결어미 (복문)
    elif has_verb:
        total_score += 28  # 동사만 존재
    elif has_adjective and has_aux:
        total_score += 25  # 형용사 + 보조용언
    elif has_adjective:
        total_score += 22  # 형용사만 존재
    elif any(tag.startswith('V') for tag in pos_tags):
        total_score += 15  # 기타 용언 (VCP, VCN 등)
    else:
        total_score += 0   # 서술어 없음
    
    # ===== 4. 문장 종결 표현 검사 (30점) =====
    has_ef = any(token.tag == 'EF' for token in tokens)  # 종결어미
    has_sf = any(token.tag == 'SF' for token in tokens)  # 문장부호
    
    # 마지막 토큰 검사
    last_token = tokens[-1]
    
    # 불완전한 종결 패턴
    incomplete_endings = ['(', ')', '[', ']', '①', '②', '③']
    
    ending_score = 0
    
    # 숫자나 기호로 끝나는 경우 (명백히 불완전)
    if last_token.tag == 'SN' or last_token.form.isdigit():
        ending_score = 0  # 숫자로 종결 (불완전)
    elif last_token.form in incomplete_endings:
        ending_score = 0  # 괄호/기호로 종결 (불완전)
    elif last_token.tag in {'JKB', 'JKO', 'JC', 'EC', 'JX'}:
        ending_score = 0  # 조사/어미로 끝남 (명백히 불완전)
    elif has_ef and has_sf:
        # 종결어미의 위치 확인
        ef_positions = [i for i, token in enumerate(tokens) if token.tag == 'EF']
        if ef_positions and ef_positions[-1] >= len(tokens) - 3:  # 끝에서 3번째 이내
            ending_score = 30  # 완벽한 종결
        else:
            ending_score = 25  # 종결 표현은 있지만 위치가 애매
    elif has_ef:
        # 종결어미가 끝부분에 있는지 확인
        ef_positions = [i for i, token in enumerate(tokens) if token.tag == 'EF']
        if ef_positions and ef_positions[-1] >= len(tokens) - 2:
            ending_score = 23  # 종결어미는 있지만 문장부호 없음
        else:
            ending_score = 15  # 종결어미가 중간에 있음
    elif has_sf and last_token.tag == 'SF':
        # 문장부호로 끝나는 경우
        if any(token.tag.startswith('N') for token in tokens[-3:]):
            ending_score = 15  # 명사 + 문장부호 (제목/표제)
        else:
            ending_score = 12  # 기타 + 문장부호
    elif last_token.tag.startswith('N'):
        ending_score = 8   # 명사로 종결 (제목/표제 가능성)
    else:
        ending_score = 5   # 기타
    
    total_score += ending_score
    
    # ===== 5. 구조적 완성도 검사 (10점) =====
    # 최소 길이 확인
    token_count = len(tokens)
    
    structure_score = 0
    
    # 적절한 문장 길이
    if token_count >= 5:
        structure_score = 10
    elif token_count >= 3:
        structure_score = 7
    elif token_count >= 2:
        structure_score = 4
    else:
        structure_score = 0
    
    # 감점 요소 1: 특수문자만 있는 경우
    non_symbol_tokens = [t for t in tokens if not t.tag.startswith('S')]
    if len(non_symbol_tokens) == 0:
        total_score = min(total_score, 10)  # 최대 10점으로 제한
    
    # 감점 요소 2: 괄호/번호가 과도하게 많은 경우 (목록/나열)
    bracket_count = sum(1 for t in tokens if t.form in ['(', ')', '[', ']', '①', '②', '③', '④', '⑤'])
    number_count = sum(1 for t in tokens if t.tag == 'SN' or t.form.isdigit())
    
    if bracket_count >= 3 or number_count >= 3:
        structure_score = max(0, structure_score - 5)  # 5점 감점
    
    total_score += structure_score
    
    return total_score


if __name__ == "__main__":
    # 테스트
    kiwi = Kiwi()
    
    test_sentences = [
        # === 완전한 문장들 (80점 이상 예상) ===
        "국제노동기구는 이러한 신청을 환영합니다.",
        "이 회의는 정부, 사업주 및 근로자 그룹을 대표하는 전문가들로 구성되었다.",
        "권한있는 기관은 선박건조 및 수리시설의 산업안전보건에 대한 법령을 제정·유지·관리해야 한다.",
        "그 대표는 교육과 훈련 프로그램의 효과성을 검토해야 한다.",
        
        # === 불완전한 문장들 (50점 미만 예상) ===
        "와 그 대표는 교육과 훈련 프로그램의 효과성을 검토해야 한다.",  # 접속사로 시작
        "(1) 건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준 (2) 물질의 유해성 여부 결정을 위해 필요한 정보의 적절성 평가 시스템 및 기준 4",  # 번호 시작 + 숫자 끝
        "xiv",  # 로마숫자만
        "숙소 ············································································································································································ 114",  # 점선 + 숫자
        "참고문헌 ························································································································································· 116",  # 점선 + 숫자
        "소 개 1.",  # 숫자로 끝
        
        # === 중간 수준 (50-70점 예상) ===
        "권한있는 기관: 법적 구속력이 있는 법령, 명령 또는 지시를 공표 및 이행할 수 있는 부처, 정부부서 또는 기타 공공기관",
        "선박건조 및 수리에 사용되는 물질은 이 요건에 따라 표시 및 표기되어야 한다.",
        "건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준",  # 명사구
        "이것은 중요한 회의이지만 (결론은 나지 않았다",
        "이럴수가!!!!!!!!",
        "권한있는 기관: 법적 구속력이 있는 법령, 명령 또는 지시를 공표 및 이행할 수 있는 부처, 정부부서 또는 ",
        "선박건조 및 수리에 사용되는 물질은 이 요건에 따라 표시 및 표기되어야 한다.",
        "건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준"
    ]
    
    print("=" * 70)
    print("문장 완결성 점수 계산 테스트 (개선된 버전)")
    print("=" * 70)
    
    for sentence in test_sentences:
        score = sentence_completeness_score(sentence, kiwi)
    
        print(f"\n문장: {sentence[:60]}")
        print(f"스코어: {score}")
    
