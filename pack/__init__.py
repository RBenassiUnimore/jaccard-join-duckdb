import string


class Tokenizer:

    def __init__(self, query):
        self.__query = query

    def get_query(self):
        return self.__query


class QGramsTokzr(Tokenizer):

    def __init__(self, q=3):
        super().__init__(
            "select distinct rid, rlen "
            f", substring(concat(repeat('#', {q} - 1), "
            "lower(val), "
            f"repeat('#',{q} - 1)),"
            f"x, {q}) as token "
            "from ("
            f"select *, len(val) + {q} - 1 as rlen, unnest(generate_series(1, rlen)) as x "
            "from input "
            ")"
        )


class WordsTokzr(Tokenizer):

    # not splitting on single quote (') yet
    default_seps = """'[!"#$%&()*+,-./:;<=>?@[\]^_`{|}~""" + string.whitespace + """]'"""

    def __init__(self, separators=default_seps):
        super().__init__(
            "select rid, len(tks) as rlen, unnest(tks) as token "
            "from ( "
            r"""select distinct rid, str_split_regex(val, """ + separators + """) as tks """
            "from input "
            ") "
        )
