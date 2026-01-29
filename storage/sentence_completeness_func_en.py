# -*- coding: utf-8 -*-
"""
english_sentence_completeness.py - 영어 문장 완결성 분석

spaCy 분석 로그를 기반으로 영어 문장의 구조적 완결성을 평가합니다.
Python 내장 라이브러리만 사용하여 패턴 매칭 기반 분석을 수행합니다.

사용법:
    from english_sentence_completeness import EnglishCompletenessScorer
    
    scorer = EnglishCompletenessScorer()
    score = scorer.calculate_score("Your sentence here.")
    print(f"완결성 점수: {score}/100")
"""

import re
from typing import Dict, List, Tuple, Optional


class EnglishCompletenessScorer:
    """
    영어 문장 완결성 점수 계산기
    
    평가 기준:
    1. 문장 시작 (15점) - 접속사, 전치사로 시작하는지 확인
    2. 주어 존재 (10점) - 대명사, 명사 패턴 확인
    3. 서술어 존재 및 품질 (35점) - 동사 형태 확인
    4. 문장 종결 표현 (30점) - 마지막 단어와 구두점 확인
    5. 구조적 완성도 (10점) - 단어 수, 괄호 짝 등
    """
    
    def __init__(self):
        """문법 패턴 및 단어 리스트 초기화"""
        # 등위접속사 (Coordinating Conjunctions)
        self.coordinating_conjunctions = {
            'and', 'but', 'or', 'nor', 'for', 'yet', 'so'
        }
        
        # 종속접속사 (Subordinating Conjunctions)
        self.subordinating_conjunctions = {
            'because', 'if', 'when', 'while', 'although', 'though',
            'unless', 'until', 'after', 'before', 'since', 'as',
            'whereas', 'wherever', 'whenever', 'whether'
        }
        
        # 전치사 (Prepositions)
        self.prepositions = {
            'in', 'on', 'at', 'to', 'for', 'with', 'about', 'from',
            'of', 'by', 'into', 'through', 'during', 'including',
            'between', 'among', 'under', 'over', 'above', 'below',
            'across', 'against', 'along', 'around', 'behind', 'beside',
            'besides', 'beyond', 'despite', 'down', 'except', 'inside',
            'near', 'off', 'onto', 'outside', 'past', 'toward', 'towards',
            'underneath', 'unlike', 'up', 'upon', 'within', 'without'
        }
        
        # 관사 (Articles)
        self.articles = {'a', 'an', 'the'}
        
        # 대명사 (Pronouns)
        self.pronouns = {
            'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'me', 'him', 'her', 'us', 'them',
            'my', 'your', 'his', 'her', 'its', 'our', 'their',
            'mine', 'yours', 'hers', 'ours', 'theirs',
            'this', 'that', 'these', 'those',
            'who', 'whom', 'whose', 'which', 'what'
        }
        
        # 조동사 (Auxiliary Verbs)
        self.auxiliaries = {
            'am', 'is', 'are', 'was', 'were', 'been', 'being',
            'have', 'has', 'had', 'having',
            'do', 'does', 'did', 'doing',
            'will', 'would', 'shall', 'should',
            'can', 'could', 'may', 'might', 'must',
            'ought', 'need', 'dare'
        }
        
        # 동사 과거형 패턴 (정규 표현식)
        self.verb_patterns = {
            'past_ed': re.compile(r'\w+ed$'),  # walked, played
            'irregular': {  # 불규칙 동사 과거형 (일부)
                'went', 'came', 'saw', 'made', 'took', 'gave', 'found',
                'told', 'got', 'became', 'left', 'felt', 'kept', 'held',
                'began', 'ran', 'stood', 'understood', 'brought', 'thought',
                'wrote', 'sat', 'heard', 'let', 'met', 'put', 'read',
                'said', 'spoke', 'knew', 'grew', 'threw', 'wore', 'won'
            },
            'present_s': re.compile(r'\w+s$'),  # walks, plays (3인칭 단수)
            'present_es': re.compile(r'\w+(sh|ch|x|z|o)es$'),  # watches, goes
            'ing': re.compile(r'\w+ing$'),  # walking, playing
        }
        
        # 명백히 불완전한 종결 단어
        self.incomplete_endings = {
            'or', 'and', 'but', 'nor', 'yet', 'so',  # 접속사
            'either', 'neither', 'both',  # 상관접속사
            'very', 'too', 'quite', 'rather', 'fairly'  # 정도 부사
        }
    
    def calculate_score(self, text: str) -> int:
        """
        영어 문장의 완결성 점수를 계산 (0-100점)
        
        Args:
            text: 검사할 영어 문장
            
        Returns:
            int: 완결성 점수 (0-100)
        """
        text = text.strip()
        if not text:
            return 0
        
        # 토큰화 (간단한 공백 기반)
        tokens = self._tokenize(text)
        if not tokens:
            return 0
        
        # 소문자 토큰 리스트 (검사용)
        tokens_lower = [t.lower() for t in tokens]
        
        total_score = 0
        
        # ===== 1. 문장 시작 검사 (15점) =====
        total_score += self._check_sentence_start(tokens, tokens_lower)
        
        # ===== 2. 주어 존재 검사 (10점) =====
        total_score += self._check_subject(tokens, tokens_lower)
        
        # ===== 3. 서술어 존재 및 품질 검사 (35점) =====
        predicate_score = self._check_predicate(tokens, tokens_lower)
        total_score += predicate_score
        
        # ===== 4. 문장 종결 표현 검사 (30점) =====
        ending_score = self._check_sentence_ending(tokens, tokens_lower, text)
        total_score += ending_score
        
        # ===== 5. 구조적 완성도 검사 (10점) =====
        total_score += self._check_structure(tokens, text)
        
        # ===== 6. 종속절 처리 =====
        total_score = self._apply_clause_penalties(tokens, tokens_lower, total_score, text)
        
        # ===== 7. 추가 패널티 적용 =====
        # 종결어미(완전한 동사 형태) 없으면 점수 제한
        has_complete_verb = self._has_complete_verb_form(tokens, tokens_lower)
        has_proper_ending = text.rstrip()[-1] in {'.', '?', '!'} if text.rstrip() else False
        
        if not has_complete_verb and not has_proper_ending:
            total_score = min(total_score, 55)
        
        return max(0, min(100, total_score))
    
    def _tokenize(self, text: str) -> List[str]:
        """간단한 토큰화 (공백 및 구두점 분리)"""
        # 구두점 앞뒤에 공백 추가
        text = re.sub(r'([.!?,;:()\[\]{}])', r' \1 ', text)
        # 연속 공백 제거
        text = re.sub(r'\s+', ' ', text)
        # 토큰 분리
        tokens = text.strip().split()
        return tokens
    
    def _check_sentence_start(self, tokens: List[str], tokens_lower: List[str]) -> int:
        """문장 시작 검사"""
        if not tokens:
            return 0
        
        first_word = tokens_lower[0]
        
        # 등위접속사로 시작 (명백한 불완전 문장)
        if first_word in self.coordinating_conjunctions:
            return 0
        
        # 분사로 시작하는 경우 (분사구문 - 주절이 없으면 불완전)
        # Having, Being, Thinking, Walking, Running, Waiting 등
        if self.verb_patterns['ing'].match(first_word):
            # -ing로 시작하면 분사구문일 가능성 높음
            return 3
        
        # 과거분사로 시작 (Broken, Located, Given 등)
        # 수동태 분사구문일 가능성
        past_participle_starts = {
            'broken', 'given', 'taken', 'made', 'done', 'seen', 'located',
            'situated', 'finished', 'completed', 'written', 'spoken'
        }
        if (self.verb_patterns['past_ed'].match(first_word) or 
            first_word in past_participle_starts):
            return 3
        
        # 전치사로 시작 (종속절일 가능성)
        if first_word in self.prepositions:
            # After, Before 등은 종속접속사로도 쓰임
            if first_word in self.subordinating_conjunctions:
                return 5
            return 3
        
        # 구두점으로 시작
        if first_word in {'.', ',', '!', '?', ';', ':'}:
            return 0
        
        # 종속접속사로 시작 (종속절만 있을 가능성)
        if first_word in self.subordinating_conjunctions:
            return 5
        
        # 대명사, 관사로 시작 (정상)
        if first_word in self.pronouns or first_word in self.articles:
            return 15
        
        # 명사로 시작 (대문자로 시작하거나 일반 단어)
        if tokens[0][0].isupper() or first_word.isalpha():
            return 15
        
        # 숫자나 기호로 시작
        if first_word.isdigit() or first_word in {'(', '[', '①', '②', '1)', '2)'}:
            return 3
        
        return 10
    
    def _check_subject(self, tokens: List[str], tokens_lower: List[str]) -> int:
        """주어 존재 검사"""
        # 대명사가 있으면 주어로 간주
        has_pronoun = any(word in self.pronouns for word in tokens_lower)
        if has_pronoun:
            return 10
        
        # 명사로 보이는 단어 찾기 (대문자로 시작하거나 관사 뒤)
        has_noun = False
        for i, word in enumerate(tokens):
            # 대문자로 시작 (고유명사)
            if word[0].isupper() and word.lower() not in self.subordinating_conjunctions:
                has_noun = True
                break
            # 관사 뒤의 단어
            if i > 0 and tokens_lower[i-1] in self.articles:
                has_noun = True
                break
        
        if has_noun:
            return 8
        
        # 명령문 확인 (동사 원형으로 시작)
        if self._is_imperative(tokens, tokens_lower):
            return 8
        
        return 0
    
    def _is_imperative(self, tokens: List[str], tokens_lower: List[str]) -> bool:
        """명령문 여부 확인"""
        if not tokens:
            return False
        
        first_word = tokens_lower[0]
        
        # "Please"로 시작하면 명령문 가능성 높음
        if first_word == 'please':
            return True
        
        # "Don't", "Do"로 시작하면 명령문
        if first_word in {"don't", "do", "doesn't", "did"}:
            return True
        
        # 일반적인 명령문 동사들
        imperative_verbs = {
            'close', 'open', 'take', 'give', 'put', 'make', 'get', 'come', 'go',
            'stop', 'start', 'turn', 'move', 'look', 'listen', 'wait', 'help',
            'try', 'use', 'keep', 'let', 'leave', 'follow', 'remember', 'forget'
        }
        
        if first_word in imperative_verbs:
            return True
        
        # 동사 원형으로 시작하고 주어가 없으면 명령문 가능성
        if first_word not in self.auxiliaries and first_word not in self.pronouns:
            # 일반 동사 패턴 (과거형이나 3인칭 단수가 아닌)
            if not (self.verb_patterns['past_ed'].match(first_word) or 
                    first_word in self.verb_patterns['irregular'] or
                    self.verb_patterns['present_s'].match(first_word)):
                # 'you'가 없으면 명령문일 가능성
                if 'you' not in tokens_lower:
                    return True
        
        return False
    
    def _check_predicate(self, tokens: List[str], tokens_lower: List[str]) -> int:
        """서술어 존재 및 품질 검사"""
        # 조동사 확인
        has_auxiliary = any(word in self.auxiliaries for word in tokens_lower)
        
        # be동사 따로 확인
        be_verbs = {'am', 'is', 'are', 'was', 'were', 'been', 'being'}
        has_be_verb = any(word in be_verbs for word in tokens_lower)
        
        # 동사 형태 확인
        has_past_verb = False
        has_present_verb = False
        has_ing_verb = False
        has_base_verb = False
        verb_count = 0
        ing_count = 0
        
        for word in tokens_lower:
            # 조동사와 be동사 제외
            if word in self.auxiliaries:
                continue
                
            # 과거형 동사
            if (self.verb_patterns['past_ed'].match(word) or 
                word in self.verb_patterns['irregular']):
                has_past_verb = True
                verb_count += 1
            # 3인칭 단수 현재형 (일반적인 복수 명사 제외)
            elif (word.endswith('s') and len(word) > 2 and
                  word not in self.prepositions and 
                  word not in {'is', 'as', 'has', 'was', 'this', 'its', 'eyes', 'days', 
                              'trees', 'doors', 'windows', 'things', 'years', 'cars',
                              'boxes', 'glasses', 'hands', 'legs', 'arms'}):
                # 동사일 가능성이 있는 -s 끝 단어
                if word not in {'always', 'perhaps', 'sometimes', 'ideas', 'times'}:
                    has_present_verb = True
                    verb_count += 1
            # -ing 형태
            elif self.verb_patterns['ing'].match(word):
                has_ing_verb = True
                ing_count += 1
                # -ing는 분사일 수 있으므로 verb_count에 조건부로만 추가
        
        # 명령문 확인
        is_imperative = self._is_imperative(tokens, tokens_lower)
        if is_imperative:
            return 35  # 명령문은 완전한 문장
        
        # 조동사가 있는 경우
        if has_auxiliary or has_be_verb:
            if has_past_verb or has_present_verb or has_ing_verb:
                return 35  # 조동사 + 본동사
            else:
                return 30  # be동사만 있는 경우도 완전할 수 있음
        
        # 과거형 동사가 있는 경우
        if has_past_verb:
            return 32
        
        # 3인칭 단수 현재형이 있는 경우
        if has_present_verb:
            return 30
        
        # -ing 형태만 있는 경우 (분사구일 가능성)
        # 단, 조동사 없이 -ing만 있으면 본동사가 아닐 가능성 높음
        if has_ing_verb and not has_past_verb and not has_present_verb:
            # ing만 여러 개 있거나 조동사 없이 단독으로 있으면 명사구/분사구일 가능성
            if ing_count >= 1 and not has_auxiliary and not has_be_verb:
                return 8  # 더 낮은 점수 (본동사 아님)
        
        # 동사가 전혀 없는 경우
        if verb_count == 0 and not has_auxiliary and not has_ing_verb:
            return 0
        
        return 10
    
    def _check_sentence_ending(self, tokens: List[str], tokens_lower: List[str], 
                               text: str) -> int:
        """문장 종결 표현 검사"""
        # 실질적 마지막 단어 찾기 (구두점 제외)
        last_meaningful_word = None
        last_meaningful_lower = None
        
        for word in reversed(tokens):
            if word not in {'.', ',', '!', '?', ';', ':', '(', ')', '[', ']'}:
                last_meaningful_word = word
                last_meaningful_lower = word.lower()
                break
        
        if not last_meaningful_word:
            return 0
        
        # 마지막 문자 확인
        last_char = text.rstrip()[-1] if text.rstrip() else ''
        has_end_punct = last_char in {'.', '?', '!'}
        is_question = last_char == '?'
        is_exclamation = last_char == '!'
        
        # === 명백히 불완전한 종결 패턴 ===
        
        # 1. 전치사로 끝남
        if last_meaningful_lower in self.prepositions:
            return -30
        
        # 2. 등위접속사로 끝남
        if last_meaningful_lower in self.coordinating_conjunctions:
            return -30
        
        # 3. 종속접속사로 끝남
        if last_meaningful_lower in self.subordinating_conjunctions:
            return -25
        
        # 4. 'to'로 끝남 (부정사 표지)
        if last_meaningful_lower == 'to':
            return -25
        
        # 4-1. 'that'으로 끝남 (불완전한 종결)
        if last_meaningful_lower == 'that':
            return -25
        
        # 4-2. 'which', 'who', 'whom'으로 끝남
        if last_meaningful_lower in {'which', 'who', 'whom', 'whose'}:
            return -25
        
        # 5. 관사로 끝남
        if last_meaningful_lower in self.articles:
            return -20
        
        # 6. 불완전한 종결 단어
        if last_meaningful_lower in self.incomplete_endings:
            return -25
        
        # 7. -ing 형태로 끝남 (구두점 없이)
        if (self.verb_patterns['ing'].match(last_meaningful_lower) and 
            not has_end_punct):
            return -20
        
        # === 완전한 종결 패턴 ===
        
        # 감탄문 확인 (What, How로 시작하고 !로 끝남)
        first_word = tokens_lower[0] if tokens else ''
        is_exclamatory = (first_word in {'what', 'how'} and is_exclamation)
        
        if is_exclamatory:
            return 30  # 감탄문은 완전한 문장
        
        # 의문문 확인
        question_words = {'what', 'where', 'when', 'why', 'who', 'whom', 'which', 'how'}
        is_question_sentence = (first_word in question_words and is_question)
        
        if is_question_sentence:
            return 30  # 의문문은 완전한 문장
        
        # 명령문 확인
        is_imperative = self._is_imperative(tokens, tokens_lower)
        if is_imperative and has_end_punct:
            return 30
        
        # 완전한 동사 형태 확인
        has_complete_verb = (
            self.verb_patterns['past_ed'].match(last_meaningful_lower) or
            last_meaningful_lower in self.verb_patterns['irregular'] or
            self.verb_patterns['present_s'].match(last_meaningful_lower) or
            last_meaningful_lower in self.auxiliaries
        )
        
        # be동사 확인
        be_verbs = {'am', 'is', 'are', 'was', 'were'}
        has_be_in_sentence = any(word in be_verbs for word in tokens_lower)
        
        # 1. 구두점 + 완전한 동사 또는 be동사
        if has_end_punct and (has_complete_verb or has_be_in_sentence):
            return 30
        
        # 2. 구두점만 (형용사/명사로 끝나는 경우)
        if has_end_punct:
            # 감탄문 가능성
            if is_exclamation:
                return 25
            return 20
        
        # 3. 완전한 동사만 (구두점 없음)
        if has_complete_verb or has_be_in_sentence:
            return 22
        
        # 4. 명사로 끝남
        if (last_meaningful_word[0].isupper() or 
            last_meaningful_lower not in self.prepositions):
            if has_end_punct:
                return 15
            else:
                return 8
        
        return 10
    
    def _check_structure(self, tokens: List[str], text: str) -> int:
        """구조적 완성도 검사"""
        # 구두점 제외한 실질 토큰 수
        real_tokens = [t for t in tokens if t not in {'.', ',', '!', '?', ';', ':', '(', ')', '[', ']'}]
        token_count = len(real_tokens)
        
        score = 0
        
        # 토큰 수에 따른 점수
        if token_count >= 5:
            score = 10
        elif token_count >= 3:
            score = 7
        elif token_count >= 2:
            score = 4
        else:
            score = 2
        
        # 괄호 짝 검사
        if not self._check_bracket_balance(text):
            return 0
        
        # 과도한 구두점 검사
        punct_count = len(tokens) - len(real_tokens)
        if punct_count > len(real_tokens) * 0.5:
            score = max(0, score - 3)
        
        return score
    
    def _check_bracket_balance(self, text: str) -> bool:
        """괄호 짝 검사"""
        stack = []
        pairs = {'(': ')', '[': ']', '{': '}'}
        
        for char in text:
            if char in pairs:
                stack.append(char)
            elif char in pairs.values():
                if not stack:
                    return False
                open_bracket = stack.pop()
                if pairs[open_bracket] != char:
                    return False
        
        return len(stack) == 0
    
    def _apply_clause_penalties(self, tokens: List[str], tokens_lower: List[str], 
                                score: int, text: str) -> int:
        """종속절 및 불완전 구문 패널티 적용"""
        if not tokens:
            return score
        
        first_word = tokens_lower[0]
        
        # 분사로 시작하는 경우 (분사구문) - 강력한 감점
        if self.verb_patterns['ing'].match(first_word):
            # Having, Being, Thinking, Walking 등
            # 쉼표가 있으면 주절이 있을 가능성
            has_comma = ',' in tokens
            if not has_comma:
                score = min(score, 45)  # 분사구만 있으면 불완전
        
        # 과거분사로 시작 (Broken, Located 등)
        past_participle_starts = {
            'broken', 'given', 'taken', 'made', 'done', 'seen', 'located',
            'situated', 'finished', 'completed', 'written', 'spoken'
        }
        if (self.verb_patterns['past_ed'].match(first_word) or 
            first_word in past_participle_starts):
            has_comma = ',' in tokens
            if not has_comma:
                score = min(score, 45)
        
        # 관계대명사 패턴 감지
        # "The book which/that/who + 동사" 형태 (주절 없이 관계절만)
        has_relative_pronoun = any(word in {'which', 'that', 'who', 'whom', 'whose'} 
                                   for word in tokens_lower)
        
        if has_relative_pronoun:
            # 관계대명사 위치 찾기
            rel_positions = [i for i, word in enumerate(tokens_lower) 
                           if word in {'which', 'that', 'who', 'whom', 'whose'}]
            
            for pos in rel_positions:
                # 관계대명사 앞에 명사가 있는지 확인
                if pos > 0:
                    # 관계대명사 뒤에 동사가 있지만 주절의 동사가 없을 가능성
                    # 전체 문장에서 관계대명사 앞부분만으로는 완전한 문장이 아님
                    
                    # 간단한 휴리스틱: 관계대명사 뒤에만 동사가 있고
                    # 앞부분에는 동사가 없으면 불완전
                    before_rel = tokens_lower[:pos]
                    after_rel = tokens_lower[pos+1:]
                    
                    # 앞부분에 동사가 있는지 확인
                    has_verb_before = any(
                        word in self.auxiliaries or
                        self.verb_patterns['past_ed'].match(word) or
                        word in self.verb_patterns['irregular']
                        for word in before_rel
                    )
                    
                    # 관계대명사 뒤에 동사가 있는지 확인
                    has_verb_after = any(
                        word in self.auxiliaries or
                        self.verb_patterns['past_ed'].match(word) or
                        word in self.verb_patterns['irregular'] or
                        self.verb_patterns['ing'].match(word)
                        for word in after_rel
                    )
                    
                    # 뒤에만 동사가 있으면 관계절만 있는 불완전 문장
                    if has_verb_after and not has_verb_before:
                        score = min(score, 48)
        
        # 종속접속사로 시작하는 경우
        if first_word in self.subordinating_conjunctions:
            # 쉼표가 있으면 주절이 있을 가능성 (감점 완화)
            has_comma = ',' in tokens
            if not has_comma:
                # 종속절만 있는 불완전 문장
                score = min(score, 50)
        
        # "Even though" 같은 복합 종속접속사
        if len(tokens) >= 2 and first_word == 'even' and tokens_lower[1] in {'though', 'if', 'when'}:
            has_comma = ',' in tokens
            if not has_comma:
                score = min(score, 48)
        
        # 관계대명사로 시작하는 경우 (관계절만 있는 불완전 문장)
        if first_word in {'who', 'which', 'that', 'whom', 'whose'}:
            score = min(score, 45)
        
        # 전치사구로만 이루어진 경우
        preposition_count = sum(1 for word in tokens_lower if word in self.prepositions)
        if preposition_count > len(tokens) * 0.4:  # 40% 이상이 전치사
            score = min(score, 45)
        
        # "To + 동사" 형태 (부정사구만)
        if first_word == 'to' and len(tokens) > 1:
            # 주절이 없으면 불완전
            # 간단한 확인: 조동사나 명확한 주절 동사가 있는지
            main_verb_found = False
            for i, word in enumerate(tokens_lower):
                if i > 1 and word in self.auxiliaries:  # to 다음의 조동사
                    continue
                if i > 1 and (word in self.verb_patterns['irregular'] or 
                             self.verb_patterns['past_ed'].match(word)):
                    main_verb_found = True
                    break
            
            if not main_verb_found:
                score = min(score, 48)
        
        # 명사구만 있는 패턴 감지 (동사가 -ing 형태만 있고 조동사 없음)
        # 예: "A red car driving", "The sound blowing", "The girl with..."
        has_real_verb = any(
            word in self.auxiliaries or
            word in self.verb_patterns['irregular'] or
            self.verb_patterns['past_ed'].match(word)
            for word in tokens_lower
        )
        
        # 3인칭 단수 현재형 확인 (lives, goes 등)
        has_present_s = any(
            word.endswith('s') and len(word) > 2 and
            word not in self.prepositions and
            word not in {'is', 'as', 'has', 'was', 'this', 'its', 'eyes', 'days', 
                        'trees', 'doors', 'windows', 'things', 'years', 'lives'}
            for word in tokens_lower
        )
        
        # 진짜 동사가 없고 -ing만 있으면 명사구/분사구
        ing_count = sum(1 for word in tokens_lower if self.verb_patterns['ing'].match(word))
        
        if not has_real_verb and not has_present_s and ing_count > 0:
            # 조동사 없이 -ing만 있으면 명사구일 가능성
            score = min(score, 55)
        
        # 구두점이 없고 동사가 없으면 명사구
        has_punct = text.rstrip()[-1] in {'.', '?', '!'} if text.rstrip() else False
        if not has_punct and not has_real_verb and not has_present_s:
            score = min(score, 55)
        
        return score
    
    def _has_complete_verb_form(self, tokens: List[str], tokens_lower: List[str]) -> bool:
        """완전한 동사 형태가 있는지 확인"""
        for word in tokens_lower:
            if (self.verb_patterns['past_ed'].match(word) or
                word in self.verb_patterns['irregular'] or
                self.verb_patterns['present_s'].match(word) or
                word in self.auxiliaries):
                return True
        return False
    
    def analyze_with_details(self, text: str) -> Dict:
        """상세 분석 결과 반환"""
        tokens = self._tokenize(text)
        tokens_lower = [t.lower() for t in tokens]
        
        # 각 항목별 점수 계산
        start_score = self._check_sentence_start(tokens, tokens_lower)
        subject_score = self._check_subject(tokens, tokens_lower)
        predicate_score = self._check_predicate(tokens, tokens_lower)
        ending_score = self._check_sentence_ending(tokens, tokens_lower, text)
        structure_score = self._check_structure(tokens, text)
        
        total_score = start_score + subject_score + predicate_score + ending_score + structure_score
        total_score = self._apply_clause_penalties(tokens, tokens_lower, total_score, text)
        total_score = max(0, min(100, total_score))
        
        return {
            'sentence': text,
            'total_score': total_score,
            'start_score': start_score,
            'subject_score': subject_score,
            'predicate_score': predicate_score,
            'ending_score': ending_score,
            'structure_score': structure_score,
            'token_count': len([t for t in tokens if t.isalpha()]),
            'has_end_punctuation': text.rstrip()[-1] in {'.', '?', '!'} if text.rstrip() else False,
            'starts_with_subordinator': tokens_lower[0] in self.subordinating_conjunctions if tokens else False,
            'ends_with_preposition': tokens_lower[-1] in self.prepositions if tokens and tokens[-1].isalpha() else False,
        }

def main():
    """테스트 실행"""
    print("=" * 80)
    print("영어 문장 완결성 분석기")
    print("=" * 80)
    
    scorer = EnglishCompletenessScorer()
    
    # 테스트 문장들
    test_sentences = [
    "Deck sockets like the container corner fitting contain the standard ISO hole into which the stackers fit (see Figure 25.5 ).",
    "RINA Conference on RO RO Safety, June 1996.",
    "Code of Safe Practice for Cargo Stowage and Securing .",
    "House DJ: Cargo Work for Maritime Operations , ed 7.",
    "Knot JR: Lashing and Securing of Deck Cargoes , ed 3, London, 2002, The Nautical Institute.",
    "Some useful websites [www.tts-se.com](http://www.tts-se.com) For details of ramps, doors, lifts, car decks, side loading systems, etc.",
    "It is easy to see the reason for this if the Aquitania , built in 1914 and having direct drive turbines with 21 double-ended scotch boilers, is compared with the Queen Elizabeth 2 .",
    "Tween deck clearances are greater and public rooms extend through two or more decks, whilst enclosed promenade and atrium spaces are now common in these vessels.",
    "Initially relatively small, these craft may now be more than 100 meters in length and carry upwards of 500 persons plus 100 cars/30 trucks or more.",
    "The latter have a high proportion of their twin-hull buoyancy below the waterline (see Figure 3.7 ).",
    "An updated version of this Code of Safety was adopted in December 2000.",
    "All rights reserved.",
    "One suction is from the main bilge line and the other from an independent power-driven pump.",
    "Bilge suctions As the vessel is to be pumped out when listed it is necessary to fit port and starboard suctions in other than very narrow spaces.",
    "However, where a ship has a single hold that exceeds 33.5m in length suctions are also arranged in the forward half length of the hold.",
    "Adequate drainage to the bilge is provided where a ceiling covers this space.",
    "If, however, the tank top extends to the ship sides, bilge wells having a capacity of at least 0.17 m 3 may be arranged in the wings of the compartment.",
    "The shaft tunnel of the ship is drained by means of a well located at the after end, and the bilge suction is taken from the main bilge line (see Figure 26.1 ).",
    "The strum box is a perforated plate box welded to the mouth of the bilge line ( Figure 26.1 ), which prevents debris being taken up by the bilge pump suction.",
    "Perforations in the strum box do not exceed 10 mm in diameter, and their total cross-sectional area is at least twice that required for the bore of the bilge pipe.",
    "Strums are arranged at a reasonable height above the bottom of the bilge or drain well to allow a clear flow of water and to permit easy cleaning.",
    "In the machinery space and shaft tunnel the pipe from the bilges is led to the mud box, which is accessible for regular cleaning.",
    "Each mud box contains a mesh to collect sludge and foreign objects entering the end of the pipe.",
    "Where the passenger ship has a length in excess of 91.5 m it is a requirement that at least one of these pumps will always be serviceable in reasonable damage situations.",
    "A submersible pump may be supplied with its source of power above the bulkhead deck.",
    "No. 5 Tunnel hold Engine room No. 1 hold No. 2 hold No. 3 hold No. 4 hold BILGE STRUM Bilge pipe Side of superstructure SCUPPER ABOVE FREEBOARD DECK",
    "Suction connections are led to each hold or compartment from the main bilge line.",
    "Bilge mains in passenger ships are kept within 20% of the ship’s beam of the side shell, and any piping outside this region or in a duct keel is fitted with a nonreturn valve.",
    "Bilge and ballast piping may be of cast or wrought iron, steel, copper, or other approved materials.",
    "Scuppers Scuppers are fitted at the ship’s side to drain the decks.",
    "Figure 26.1 shows a scupper fitted above the freeboard deck.",
    "Below the freeboard deck and within the intact superstructures and houses on the freeboard deck fitted with weathertight doors, these scuppers may be led to the bilges.",
    "It is a statutory requirement that a fire main and deck wash system should be supplied.",
    "This has hose outlets on the various decks, and is supplied by power-driven pumps in the machinery spaces.",
    "Provision may be made for washing down the anchor chain from a connection to the fire main.",
    "Air and sounding pipes Air pipes are provided for all tanks to prevent air being trapped under pressure in the tank when it is filled, or a vacuum being created when it is emptied.",
    "Each air pipe from a double-bottom tank, deep tanks that extend to the ship’s side, or any tank that may be run up from the sea, is led up above the bulkhead deck.",
    "From oil fuel and cargo oil tanks, cofferdams, and all tanks that can be pumped up, the air pipes are led to an open deck, in a position where no danger will result from leaking oil or vapors.",
    "Sounding pipes are provided to all tanks, and compartments not readily accessible and are located so that soundings are taken in the vicinity of the suctions, i.e. at the lowest point of the tank.",
    "Each sounding pipe is made as straight as possible and is led above the bulkhead deck, except in some machinery spaces where this might not be practicable.",
    "Underneath the sounding pipe a striking plate is provided where the sounding rod drops in the bilge well etc.",
    "a substantial box within the line of the shell plate containing the sea inlet opening (see Figure 26.2 ).",
    "This opening is to have rounded corners and be kept clear of the bilge strake if possible.",
    "The sea inlet box should have the same thickness as the adjacent shell but is not to be less than 12.5 mm thick and need not exceed 25 mm.",
    "Where smaller tankers carry a number of oil products at one time, which must be kept separate, the pumping system is more complex.",
    "The piping system is of the ‘direct line’ type, three or four lines being provided, each with Anode Sea inlet box Sea inlet grids positioned so that bars are in fore and aft direction Closing valve Mud box Sluice valve Figure 26.2 Sea inlet.",
    "suctions from a group of tanks (see Figure 26.3 ).",
    "Each pump discharge is led up to the deck mains, which run forward to the transverse loading and discharging connections.",
    "These permit a flow of the oil to a common suction in the after tank space.",
    "Suctions from the main cargo lines are located in the center tanks.",
    "Such an arrangement is shown in Figure 26.3 , which also indicates the separate stripping system, and clean ballast lines.",
    "One may be fitted aft adjacent to the machinery space, a second amidships, and where a third pump room is provided this is forward.",
    "On many older tankers the piping was often arranged on the ‘ring main’ system to provide flexibility of pumping conditions (see Figure 26.4 ).",
    "1 2 3 9 8 7 6 5 4 3 2 1 Machinery Pump room CARGO PIPING WITH PARTIAL FULL FLOW IN LARGE CRUDE OIL TANKER Pump room Water ballast Water ballast Filling lines Deep tank 2 1 3 4 5 Stripping line Cargo line Ballast line Bulkhead valves Figure 26.4 Diagrammatic representation of ring main cargo piping arrangement.",
    "Oil tankers of 20,000 tonnes deadweight or more should have a fixed deck foam system and a fixed inert gas system.",
    "On an oil tanker, inert gas may be produced by one of two processes: 1. Ships with main or auxiliary boilers normally use the flue gas, which contains typically only 2–4% by volume of oxygen.",
    "An inert gas generating plant may then be used to produce gas by burning diesel or light fuel oil.",
    "The gas is scrubbed and used in the same way as boiler flue gas.",
    "Gas freeing may then take place at the cargo tank deck level.",
    "Cargo tank washing After discharge of cargoes, oil tanker cargo tanks can be cleaned by either hot or cold water, fresh or sea water, or by crude oil washing.",
    "At this stage the builder should have sufficient information to tender.",
    "The design of the ship is not complete at this stage, rather for the major effort in resources it has only just started.",
    "Stability Figure 1.1 Design spiral.",
    "a M n oit a d",
    "-I T L U M L",
    "There may then be no need to water wash tanks for the removal of residues unless clean water ballast is to be carried in the tank.",
    "All rights reserved.",
    "A simple corrosion cell is formed by two different metals in an electrolyte solution (a galvanic cell), as illustrated in Figure 27.1 .",
    ]
    
    results = []
    
    print("\n" + "=" * 80)
    print("분석 결과")
    print("=" * 80)
    
    below_60 = []  # 60점 미만
    above_60 = []  # 60점 이상

    for sentence in test_sentences:
        score = scorer.calculate_score(sentence)
        results.append((sentence, score))
        
        status = "✓ 완전" if score >= 60 else "✗ 불완전"
        print(f"\n[{score:3d}점] {status}")
        print(f"  {sentence}")
    
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

if __name__ == "__main__":
    main()