import string


class Tokenizer:

    def __init__(self, info):
        self.__info = info

    def get_info(self):
        return self.__info


class QGramsTokzr(Tokenizer):

    def __init__(self, q=3):
        super().__init__(q)


class WordsTokzr(Tokenizer):

    # not splitting on single quote (') yet
    default_seps = r"""'[!"#$%&()*+,-./:;<=>?@[\]^_`{|}~""" + string.whitespace + r"""]'"""

    def __init__(self, separators=default_seps):
        super().__init__(separators)
