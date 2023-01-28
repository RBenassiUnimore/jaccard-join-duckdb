import string

from py_duckdb.similarity_join.default_names import INPUT_TABLE


class Tokenizer:

    def __init__(self, query: string):
        self.__query = query

    def query(self, from_table=INPUT_TABLE):
        return self.__query.format(from_table=from_table)


class QGramsTokzr(Tokenizer):

    def __init__(self, q=3):
        super().__init__(
            "select src, rid, len(tks) as rlen, lower(unnest(tks)) as token "
            "from ( "
            f"select rid, list_distinct(list_transform(generate_series(1, len(val) + {q} - 1), x -> "
            f"substring(concat(repeat('#', {q} - 1), "
            "lower(val), "
            f"repeat('#',{q} - 1)),"
            f"x, {q}))) as tks "
            ", src "
            "from {from_table} "
            ") "
        )


class WordsTokzr(Tokenizer):
    default_seps = r"""'[!"#$%&()*+,-./:;<=>?@[\]^_`{{|}}~""" + string.whitespace + r"""]'"""

    def __init__(self, separators=default_seps):
        super().__init__(
            "select src, rid, len(tks) as rlen, lower(unnest(tks)) as token "
            "from ( "
            f"select src, rid, "
            f"list_distinct(list_filter(str_split_regex(val, {separators}), x -> trim(x) != '')) as tks """
            "from {from_table} "
            ") "
        )
