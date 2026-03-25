class Base62Converter:
    base62_charset = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

    @classmethod
    def decimal_to_base62(cls, num: int) -> str:
        if not isinstance(num, int):
            raise ValueError("Input must be an integer.")

        base = len(cls.base62_charset)

        if base == 0:
            raise ValueError('Base62 charset is invalid, base is 0.')

        if num == 0:
            return '0'

        result = ''
        while num > 0:
            remainder = num % base
            result = cls.base62_charset[remainder] + result
            num = num // base

        return result

    @classmethod
    def base62_to_decimal(cls, s: str) -> int:
        base = len(cls.base62_charset)
        char_to_value = {char: idx for idx, char in enumerate(cls.base62_charset)}

        num = 0
        for char in s:
            value = char_to_value[char]
            num = num * base + value

        return num
