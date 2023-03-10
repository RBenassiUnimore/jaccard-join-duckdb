class Tokenizer:

    def __init__(self, query: str, return_set=True):
        # assuming queries use DuckDB's 'list_distinct()' to generate a set rather than a bag
        self.__query = query if return_set else query.replace("list_distinct", "")

    def query(self, from_table, key, val):
        return self.__query.format(from_table=from_table, key=key, val=val)


class QGramsTokzr(Tokenizer):

    def __init__(self, q: int, return_set=True):
        super().__init__(
            "select {key}, len(tks) as len, lower(unnest(tks)) as token "
            "from ( "
            "select {key}, "
            f"list_distinct(list_transform(generate_series(1, len({{val}}) + {q} - 1), x -> "
            f"substring(concat(repeat('#', {q} - 1), "
            "lower({val}), "
            f"repeat('#', {q} - 1)),"
            f"x, {q}))) as tks "
            "from {from_table} "
            ") ",
            return_set
        )


class DelimiterTokzr(Tokenizer):

    def __init__(self, separators: list[str] or set[str], return_set=True):
        if isinstance(separators, list):
            separators = set(separators)
        separators = f"""[{''.join(separators)}]"""
        super().__init__(
            "select {key}, len(tks) as len, lower(unnest(tks)) as token "
            "from ( "
            "select {key}, "
            f"list_distinct(list_filter(str_split_regex({{val}}, '{separators}'), x -> trim(x) != '')) as tks """
            "from {from_table} "
            ") ",
            return_set
        )


class WhitespaceTokzr(DelimiterTokzr):

    def __init__(self, return_set=True):
        super().__init__({' ', '\t', '\r', '\n'}, return_set)
