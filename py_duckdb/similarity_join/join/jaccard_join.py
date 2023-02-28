from abc import ABC, abstractmethod

import duckdb

from py_duckdb.similarity_join import tokenizers
from py_duckdb.similarity_join.default_names import *


def jaccard_join(
        con: duckdb.DuckDBPyConnection,
        l_table: str,
        r_table: str,
        l_key_attr: str,
        r_key_attr: str,
        l_join_attr: str,
        r_join_attr: str,
        tokenizer: tokenizers.Tokenizer,
        threshold: float,
        out_table_name: str,
        l_out_prefix: str = 'l_',
        r_out_prefix: str = 'r_'
):
    if l_table:
        if l_table == r_table or not r_table:
            _JaccardSelfJoin(
                con, l_table, l_key_attr, l_join_attr, tokenizer, threshold, out_table_name, l_out_prefix, r_out_prefix
            ).do_join()
        else:
            _JaccardJoin(
                con, l_table, r_table, l_key_attr, r_key_attr, l_join_attr, r_join_attr, tokenizer, threshold,
                out_table_name, l_out_prefix, r_out_prefix
            ).do_join()
    return con


def jaccard_join_brute_force(
        con: duckdb.DuckDBPyConnection,
        l_table: str,
        r_table: str,
        l_key_attr: str,
        r_key_attr: str,
        l_join_attr: str,
        r_join_attr: str,
        tokenizer: tokenizers.Tokenizer,
        threshold: float,
        out_table_name: str,
        l_out_prefix: str = 'l_',
        r_out_prefix: str = 'r_'
):
    if l_table:
        if l_table == r_table or not r_table:
            _JaccardSelfJoin(
                con, l_table, l_key_attr, l_join_attr, tokenizer, threshold, out_table_name, l_out_prefix, r_out_prefix
            ).do_brute_force_join()
        else:
            _JaccardJoin(
                con, l_table, r_table, l_key_attr, r_key_attr, l_join_attr, r_join_attr, tokenizer, threshold,
                out_table_name, l_out_prefix, r_out_prefix
            ).do_brute_force_join()
    return con


class _JaccardTemplateJoin(ABC):

    def __init__(
            self,
            con: duckdb.DuckDBPyConnection,
            l_table: str,
            r_table: str,
            l_key_attr: str,
            r_key_attr: str,
            l_join_attr: str,
            r_join_attr: str,
            tokenizer: tokenizers.Tokenizer,
            threshold: float,
            out_table_name: str,
            l_out_prefix: str,
            r_out_prefix: str
    ):
        self._con = con
        self._l_table = l_table
        self._r_table = r_table
        self._l_key_attr = l_key_attr
        self._r_key_attr = r_key_attr
        self._l_join_attr = l_join_attr
        self._r_join_attr = r_join_attr
        self._t = threshold
        self._tokenizer = tokenizer
        self._out_table_name = out_table_name
        self._l_out_prefix = l_out_prefix
        self._r_out_prefix = r_out_prefix

        self._l_count = 0
        self._r_count = 0
        self._widow_placeholder = 0

    def do_join(self):
        self.build_input()
        self.tokenize()
        self.document_frequency()
        self.prefixes()
        self.candidates()
        self.matches()
        self.clear()

    def do_brute_force_join(self):
        self.build_input()
        self.tokenize()
        self.matches_brute_force()
        self.clear()

    @abstractmethod
    def build_input(self):
        pass

    def tokenize(self):
        self._con.execute(
            f"drop table if exists {TOKENS_VIEW}"
        ).execute(
            f"create table {TOKENS_VIEW} as " + self._tokenizer.query()
        ).execute(
            f"drop table if exists {INPUT_TABLE}"
        )

    @abstractmethod
    def document_frequency(self):
        pass

    @abstractmethod
    def prefixes(self):
        pass

    @abstractmethod
    def candidates(self):
        pass

    @abstractmethod
    def matches(self):
        pass

    @abstractmethod
    def matches_brute_force(self):
        pass

    @abstractmethod
    def clear(self):
        pass


class _JaccardSelfJoin(_JaccardTemplateJoin):

    def build_input(self):
        self._con.execute(
            f"drop table if exists {INPUT_TABLE}"
        ).execute(
            f"create table {INPUT_TABLE} as ("
            f"select '{self._l_table}' as src, {self._l_key_attr} as rid, {self._l_join_attr} as val "
            f"from '{self._l_table}' )"
        )

    def document_frequency(self):
        self._con.execute(
            f"drop table if exists {DOC_FREQ_VIEW}"
        ).execute(
            f"CREATE table {DOC_FREQ_VIEW} AS "
            "SELECT token, count(*) AS df "
            f"FROM {TOKENS_VIEW} "
            "GROUP BY token "
        ).execute(f"drop table if exists {TOKENS_DOC_FREQ_VIEW}").execute(
            f"CREATE table {TOKENS_DOC_FREQ_VIEW} AS "
            f"select rid, rlen, {TOKENS_VIEW}.token "
            f", row_number() OVER (PARTITION BY rid ORDER BY df, {TOKENS_VIEW}.token) as pos "
            f", concat(rlen, '_', rid) as lrid "
            f"from {TOKENS_VIEW}, {DOC_FREQ_VIEW} "
            f"where {TOKENS_VIEW}.token = {DOC_FREQ_VIEW}.token"
        ).execute(
            f"drop table if exists {TOKENS_VIEW}"
        ).execute(
            f"drop table if exists {DOC_FREQ_VIEW}"
        )

    def prefixes(self):
        pass

    def candidates(self):
        self._con.execute(
            f"drop table if exists {CANDIDATE_SET_VIEW}"
        ).execute(
            f"CREATE table {CANDIDATE_SET_VIEW} AS "
            "SELECT pr1.rid AS rid1, pr2.rid AS rid2 "
            ", MAX(pr1.pos) as maxPos1, MAX(pr2.pos) as maxPos2, count(*) as prOverlap "
            f"FROM {TOKENS_DOC_FREQ_VIEW} pr1, {TOKENS_DOC_FREQ_VIEW} pr2 "
            "where pr1.lrid < pr2.lrid "  # pr2 longest
            "AND pr1.token = pr2.token "
            # length filter
            f"AND pr1.rlen >= (pr2.rlen * {self._t})"  # pr2 longest
            # prefix filter
            f"AND pr1.rlen - pr1.pos + 1 >= (pr1.rlen * 2 * {self._t} / (1 + {self._t})) "  # indexing prefix
            f"AND pr2.rlen - pr2.pos + 1 >= (pr2.rlen * {self._t}) "  # probing prefix
            # positional filter
            "AND LEAST((pr1.rlen - pr1.pos + 1), (pr2.rlen - pr2.pos + 1)) >= "
            f"((pr1.rlen + pr2.rlen) * {self._t} / (1 + {self._t})) "
            "GROUP BY pr1.rid, pr2.rid "
        )

    def matches(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            # Start from the last match included to include the pairs in which the prefixes match entirely but the
            # suffixes do not match at all
            f"create table {self._out_table_name} as "
            f"select r1.rid as {self._l_out_prefix}{self._l_key_attr}, r2.rid as {self._r_out_prefix}{self._r_key_attr} "
            f"from {TOKENS_DOC_FREQ_VIEW} r1, {TOKENS_DOC_FREQ_VIEW} r2, {CANDIDATE_SET_VIEW} c "
            "where c.rid1 = r1.rid "
            "and c.rid2 = r2.rid "
            "and r1.token = r2.token "
            "and r1.pos >= maxPos1 "
            "and r2.pos >= maxPos2 "
            "group by r1.rid, r2.rid, r1.rlen, r2.rlen, prOverlap "
            f"having count(*) + prOverlap - 1 >= ((r1.rlen + r2.rlen) * {self._t} / (1+{self._t}))"
        ).execute(
            f"drop table if exists {CANDIDATE_SET_VIEW}"
        ).execute(
            f"drop table if exists {TOKENS_DOC_FREQ_VIEW}"
        )

    def matches_brute_force(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            f"create table {self._out_table_name} as "
            f"select r1.rid as {self._l_out_prefix}{self._l_key_attr}, r2.rid as {self._r_out_prefix}{self._r_key_attr} "
            f"from {TOKENS_VIEW} as r1, {TOKENS_VIEW} as r2 "
            "where r1.token = r2.token "
            "and r1.rid < r2.rid "
            "group by r1.rid, r1.rlen, r2.rid, r2.rlen "
            f"having count(*) >= ((r1.rlen + r2.rlen) * {self._t} / (1+{self._t}))"
        )

    def clear(self):
        self._con.execute(
            f"drop table if exists {INPUT_TABLE};"
            f"drop table if exists {TOKENS_VIEW};"
            f"drop table if exists {DOC_FREQ_VIEW};"
            f"drop table if exists {TOKENS_DOC_FREQ_VIEW};"
            f"drop table if exists {PREFIXES_VIEW};"
            f"drop table if exists {CANDIDATE_SET_VIEW};"
        )

    def __init__(
            self,
            con: duckdb.DuckDBPyConnection,
            table: str,
            key_attr: str,
            join_attr: str,
            tokenizer: tokenizers.Tokenizer,
            threshold: float,
            out_table_name: str,
            l_out_prefix: str,
            r_out_prefix: str
    ):
        super().__init__(
            con, table, None, key_attr, key_attr, join_attr, None, tokenizer, threshold, out_table_name,
            l_out_prefix, r_out_prefix
        )


class _JaccardJoin(_JaccardTemplateJoin):

    def build_input(self):
        self._con.execute(
            f"drop table if exists {INPUT_TABLE}"
        ).execute(
            f"create table {INPUT_TABLE} as ("
            f"select '{self._l_table}' as src, {self._l_key_attr} as rid, {self._l_join_attr} as val "
            f"from '{self._l_table}' "
            "union "
            f"select '{self._r_table}' as src, {self._r_key_attr} as rid, {self._r_join_attr} as val "
            f"from '{self._r_table}' )"
        )

        self._l_count = self._con.execute(
            "select count(*) "
            f"from {self._l_table}"
        ).fetchall()[0][0]
        self._r_count = self._con.execute(
            "select count(*) "
            f"from {self._r_table}"
        ).fetchall()[0][0]

    def document_frequency(self):
        # the document frequency of widow tokens is the max possible df + 1
        # this works both as unambiguous placeholder and to put widow tokens at the end in the ordering heustic
        self._widow_placeholder = self._l_count * self._r_count + 1

        self._con.execute(
            f"drop table if exists full_outer_{DOC_FREQ_VIEW}"
        ).execute(
            f"create table full_outer_{DOC_FREQ_VIEW} as "
            "select l_tks.token as l_tk, l_tks.df as l_df, r_tks.token as r_tk, r_tks.df as r_df "
            "from ("
            "SELECT token, count(*) AS df "
            f"FROM {TOKENS_VIEW} "
            f"where src = '{self._l_table}' "
            "GROUP BY token "
            ") as l_tks "
            "full outer join ("
            "SELECT token, count(*) AS df "
            f"FROM {TOKENS_VIEW} "
            f"where src = '{self._r_table}' "
            "GROUP BY token "
            ") as r_tks "
            "on l_tks.token = r_tks.token"
        ).execute(
            f"drop table if exists {DOC_FREQ_VIEW}"
        ).execute(
            # Include widows, with df=null
            f"create table {DOC_FREQ_VIEW} as ("
            f"select coalesce(l_tk, r_tk) as token, coalesce(l_df * r_df, {self._widow_placeholder}) as df "
            f"from full_outer_{DOC_FREQ_VIEW} "
            ")"
        ).execute(
            f"drop table if exists {TOKENS_DOC_FREQ_VIEW}"
        ).execute(
            f"create table {TOKENS_DOC_FREQ_VIEW} as "
            f"select {TOKENS_VIEW}.*, {DOC_FREQ_VIEW}.df "
            f", row_number() OVER (PARTITION BY rid ORDER BY df, {TOKENS_VIEW}.token) as pos "
            f"from {TOKENS_VIEW}, {DOC_FREQ_VIEW} "
            f"where {TOKENS_VIEW}.token = {DOC_FREQ_VIEW}.token"
        ).execute(
            f"drop table if exists {TOKENS_VIEW}"
        ).execute(
            f"drop table if exists {DOC_FREQ_VIEW}"
        )

    def prefixes(self):
        self._con.execute(
            f"drop table if exists l_{PREFIXES_VIEW}"
        ).execute(
            f"create table l_{PREFIXES_VIEW} as "
            "select * "
            f"FROM {TOKENS_DOC_FREQ_VIEW} "
            f"where src = '{self._l_table}' "
            f"and rlen - pos + 1 >= (rlen * 2 * {self._t} / (1+{self._t})) "  # indexing prefix
        ).execute(
            f"drop table if exists r_{PREFIXES_VIEW}"
        ).execute(
            f"create table r_{PREFIXES_VIEW} as "
            "select * "
            f"FROM {TOKENS_DOC_FREQ_VIEW} "
            f"where src = '{self._r_table}' "
            f"and rlen - pos + 1 >= (rlen * 2 * {self._t} / (1+{self._t})) "  # indexing prefix
        )

        l_widows = self._con.execute(
            "select count(*) "
            f"from l_{PREFIXES_VIEW} "
            f"where src = '{self._l_table}' "
            f"and df = {self._widow_placeholder}"
        ).fetchall()[0][0]

        r_widows = self._con.execute(
            "select count(*) "
            f"from r_{PREFIXES_VIEW} "
            f"where src = '{self._r_table}' "
            f"and df = {self._widow_placeholder}"
        ).fetchall()[0][0]

        r, s = (self._l_table, self._r_table) if l_widows > r_widows else (self._r_table, self._l_table)
        r_pfx, s_pfx = (f'l_{PREFIXES_VIEW}', f'r_{PREFIXES_VIEW}') if l_widows > r_widows else (
            f'r_{PREFIXES_VIEW}', f'l_{PREFIXES_VIEW}')

        self._con.execute(
            f"drop table if exists {s_pfx}"
        ).execute(
            f"create table {s_pfx} as "
            "select * "
            f"FROM {TOKENS_DOC_FREQ_VIEW} "
            f"where src = '{s}' "
            f"and rlen - pos + 1 >= (rlen * {self._t}) "  # probing prefix
        )

        self._con.execute(
            f"drop table if exists {PREFIXES_VIEW}"
        ).execute(
            f"create table {PREFIXES_VIEW} as "
            "select src, rid, rlen, token, pos "
            f"from {r_pfx} "
            "union "
            "select src, rid, rlen, token, pos "
            f"from {s_pfx} "
        ).execute(
            f"drop table if exists {r_pfx};"
            f"drop table if exists {s_pfx};"
        )

    def candidates(self):
        self._con.execute(
            f"drop table if exists {CANDIDATE_SET_VIEW}"
        ).execute(
            f"CREATE table {CANDIDATE_SET_VIEW} AS ("
            "SELECT pr1.rid AS rid1, pr2.rid AS rid2 "
            ", MAX(pr1.pos) as maxPos1, MAX(pr2.pos) as maxPos2, count(*) as prOverlap "
            f"FROM {PREFIXES_VIEW} pr1, {PREFIXES_VIEW} pr2 "
            "WHERE pr1.token = pr2.token "
            f"and pr1.src = '{self._l_table}' "
            f"and pr2.src = '{self._r_table}'"
            # length filter
            f"AND pr1.rlen >= (pr2.rlen * {self._t})"
            f"AND pr2.rlen >= (pr1.rlen * {self._t})"
            # positional filter
            "AND LEAST((pr1.rlen - pr1.pos + 1), (pr2.rlen - pr2.pos + 1)) >= "
            f"((pr1.rlen + pr2.rlen) * {self._t} / (1 + {self._t})) "
            "GROUP BY pr1.rid, pr2.rid "
            ")"
        ).execute(
            f"drop table if exists {PREFIXES_VIEW}"
        )

    def matches(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            f"create table {self._out_table_name} as "
            f"select r1.rid as {self._l_out_prefix}{self._l_key_attr}, r2.rid as {self._r_out_prefix}{self._r_key_attr} "
            f"from {TOKENS_DOC_FREQ_VIEW} r1, {TOKENS_DOC_FREQ_VIEW} r2, {CANDIDATE_SET_VIEW} c "
            "where c.rid1 = r1.rid "
            "and c.rid2 = r2.rid "
            "and r1.token = r2.token "
            "and r1.pos >= maxPos1 "
            "and r2.pos >= maxPos2 "
            "group by r1.rid, r2.rid, r1.rlen, r2.rlen, prOverlap "
            f"having count(*) + prOverlap - 1 >= ((r1.rlen + r2.rlen) * {self._t} / (1+{self._t}))"
        ).execute(
            f"drop table if exists {TOKENS_DOC_FREQ_VIEW}"
        ).execute(
            f"drop table if exists {CANDIDATE_SET_VIEW}"
        )

    def matches_brute_force(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            f"create table {self._out_table_name} as "
            f"select r1.rid as {self._l_out_prefix}{self._l_key_attr}, r2.rid as {self._r_out_prefix}{self._r_key_attr} "
            f"from {TOKENS_VIEW} as r1, {TOKENS_VIEW} as r2 "
            "where r1.token = r2.token "
            f"and r1.src = '{self._l_table}' and r2.src = '{self._r_table}' "
            "group by r1.rid, r1.rlen, r2.rid, r2.rlen "
            f"having count(*) >= ((r1.rlen + r2.rlen) * {self._t} / (1+{self._t}))"
        )

    def clear(self):
        self._con.execute(
            f"drop table if exists {INPUT_TABLE};"
            f"drop table if exists {TOKENS_VIEW};"
            f"drop table if exists full_outer_{DOC_FREQ_VIEW};"
            f"drop table if exists {DOC_FREQ_VIEW};"
            f"drop table if exists {TOKENS_DOC_FREQ_VIEW};"
            f"drop table if exists {PREFIXES_VIEW};"
            f"drop table if exists {CANDIDATE_SET_VIEW};"
        )

    def __init__(
            self,
            con: duckdb.DuckDBPyConnection,
            l_table: str,
            r_table: str,
            l_key_attr: str,
            r_key_attr: str,
            l_join_attr: str,
            r_join_attr: str,
            tokenizer: tokenizers.Tokenizer,
            threshold: float,
            out_table_name: str,
            l_out_prefix: str,
            r_out_prefix: str
    ):
        super().__init__(
            con, l_table, r_table, l_key_attr, r_key_attr, l_join_attr, r_join_attr, tokenizer, threshold,
            out_table_name, l_out_prefix, r_out_prefix
        )
