import re

def _remove_custom_kaomoji(text):
    text = re.sub(r"φ(^ω^)", "", text)
    text = re.sub(r"(^ω^)", "", text)
    text = re.sub(r'\(^^ゞ', "", text)
    text = re.sub(r"φ(^ω^)", "", text)
    
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