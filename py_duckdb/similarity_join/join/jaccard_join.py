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
        self._r = ''
        self._s = ''

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

    @abstractmethod
    def tokenize(self):
        pass

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
            f"create table {INPUT_TABLE} as "
            f"select {self._l_key_attr} as id, {self._l_join_attr} as val "
            f"from '{self._l_table}' "
        )

    def tokenize(self):
        self._con.execute(
            f"drop table if exists {TOKENS_VIEW}"
        ).execute(
            f"create table {TOKENS_VIEW} as " + self._tokenizer.query()
        ).execute(
            f"drop table if exists {INPUT_TABLE}"
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
            f"select id, len, {TOKENS_VIEW}.token "
            f", row_number() OVER (PARTITION BY id ORDER BY df, {TOKENS_VIEW}.token) as pos "
            f", concat(len, '_', id) as l_id "
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
            "SELECT L.id AS Lid, R.id AS Rid "
            ", MAX(L.pos) as LmaxPos, MAX(R.pos) as RmaxPos, count(*) as pfxOverlap "
            f"FROM {TOKENS_DOC_FREQ_VIEW} L, {TOKENS_DOC_FREQ_VIEW} R "
            "where L.l_id < R.l_id "  # pr2 longest
            "AND L.token = R.token "
            # length filter
            f"AND L.len >= (R.len * {self._t})"  # pr2 longest
            # prefix filter
            f"AND L.len - L.pos + 1 >= (L.len * 2 * {self._t} / (1 + {self._t})) "  # indexing prefix
            f"AND R.len - R.pos + 1 >= (R.len * {self._t}) "  # probing prefix
            # positional filter
            "AND LEAST((L.len - L.pos + 1), (R.len - R.pos + 1)) >= "
            f"((L.len + R.len) * {self._t} / (1 + {self._t})) "
            "GROUP BY L.id, R.id "
        )

    def matches(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            # Start from the last match included to include the pairs in which the prefixes match entirely but the
            # suffixes do not match at all
            f"create table {self._out_table_name} as "
            f"select L.id as {self._l_out_prefix}{self._l_key_attr}, R.id as {self._r_out_prefix}{self._r_key_attr} "
            f"from {TOKENS_DOC_FREQ_VIEW} L, {TOKENS_DOC_FREQ_VIEW} R, {CANDIDATE_SET_VIEW} c "
            "where c.Lid = L.id "
            "and c.Rid = R.id "
            "and L.token = R.token "
            "and L.pos >= LmaxPos "
            "and R.pos >= RmaxPos "
            "group by L.id, R.id, L.len, R.len, pfxOverlap "
            f"having count(*) + pfxOverlap - 1 >= ((L.len + R.len) * {self._t} / (1+{self._t}))"
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
            f"select L.id as {self._l_out_prefix}{self._l_key_attr}, R.id as {self._r_out_prefix}{self._r_key_attr} "
            f"from {TOKENS_VIEW} as L, {TOKENS_VIEW} as R "
            "where L.token = R.token "
            "and L.id < R.id "
            "group by L.id, L.len, R.id, R.len "
            f"having count(*) >= ((L.len + R.len) * {self._t} / (1+{self._t}))"
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
            con, table, '', key_attr, key_attr, join_attr, '', tokenizer, threshold, out_table_name,
            l_out_prefix, r_out_prefix
        )


class _JaccardJoin(_JaccardTemplateJoin):

    def build_input(self):
        self._con.execute(
            f"drop table if exists l_{INPUT_TABLE};"
            f"drop table if exists r_{INPUT_TABLE};"
        ).execute(
            f"create table l_{INPUT_TABLE} as "
            f"select {self._l_key_attr} as id, {self._l_join_attr} as val "
            f"from '{self._l_table}' "
        ).execute(
            f"create table r_{INPUT_TABLE} as "
            f"select {self._r_key_attr} as id, {self._r_join_attr} as val "
            f"from '{self._r_table}' "
        )

        self._l_count = self._con.execute(
            "select count(*) "
            f"from l_{INPUT_TABLE}"
        ).fetchall()[0][0]
        self._r_count = self._con.execute(
            "select count(*) "
            f"from r_{INPUT_TABLE}"
        ).fetchall()[0][0]

    def tokenize(self):
        self._con.execute(
            f"drop table if exists l_{TOKENS_VIEW}"
        ).execute(
            f"create table l_{TOKENS_VIEW} as " + self._tokenizer.query(f'l_{INPUT_TABLE}')
        ).execute(
            f"drop table if exists l_{INPUT_TABLE}"
        )

        self._con.execute(
            f"drop table if exists r_{TOKENS_VIEW}"
        ).execute(
            f"create table r_{TOKENS_VIEW} as " + self._tokenizer.query(f'r_{INPUT_TABLE}')
        ).execute(
            f"drop table if exists r_{INPUT_TABLE}"
        )

    def document_frequency(self):
        # the document frequency of widow tokens is the max possible df + 1
        # this works both as unambiguous placeholder and to put widow tokens at the end in the ordering heuristic
        self._widow_placeholder = self._l_count * self._r_count + 1

        self._con.execute(
            f"drop table if exists full_outer_{DOC_FREQ_VIEW}"
        ).execute(
            f"create table full_outer_{DOC_FREQ_VIEW} as "
            "select l_tks.token as l_tk, l_tks.df as l_df, r_tks.token as r_tk, r_tks.df as r_df "
            "from ("
            "SELECT token, count(*) AS df "
            f"FROM l_{TOKENS_VIEW} "
            "GROUP BY token "
            ") as l_tks "
            "full outer join ("
            "SELECT token, count(*) AS df "
            f"FROM r_{TOKENS_VIEW} "
            "GROUP BY token "
            ") as r_tks "
            "on l_tks.token = r_tks.token"
        )

        self._con.execute(
            f"drop table if exists {DOC_FREQ_VIEW}"
        ).execute(
            # Include widows, with df=null
            f"create table {DOC_FREQ_VIEW} as "
            f"select coalesce(l_tk, r_tk) as token, coalesce(l_df * r_df, {self._widow_placeholder}) as df "
            f"from full_outer_{DOC_FREQ_VIEW} "
        )

        self._con.execute(
            f"drop table if exists l_{TOKENS_DOC_FREQ_VIEW}"
        ).execute(
            f"create table l_{TOKENS_DOC_FREQ_VIEW} as "
            f"select l_{TOKENS_VIEW}.*, {DOC_FREQ_VIEW}.df "
            f", row_number() OVER (PARTITION BY id ORDER BY df, l_{TOKENS_VIEW}.token) as pos "
            f"from l_{TOKENS_VIEW}, {DOC_FREQ_VIEW} "
            f"where l_{TOKENS_VIEW}.token = {DOC_FREQ_VIEW}.token"
        ).execute(
            f"drop table if exists l_{TOKENS_VIEW}"
        )
        self._con.execute(
            f"drop table if exists r_{TOKENS_DOC_FREQ_VIEW}"
        ).execute(
            f"create table r_{TOKENS_DOC_FREQ_VIEW} as "
            f"select r_{TOKENS_VIEW}.*, {DOC_FREQ_VIEW}.df "
            f", row_number() OVER (PARTITION BY id ORDER BY df, r_{TOKENS_VIEW}.token) as pos "
            f"from r_{TOKENS_VIEW}, {DOC_FREQ_VIEW} "
            f"where r_{TOKENS_VIEW}.token = {DOC_FREQ_VIEW}.token"
        ).execute(
            f"drop table if exists r_{TOKENS_VIEW}"
        )

        self._con.execute(
            f"drop table if exists {DOC_FREQ_VIEW}"
        )

    def prefixes(self):
        self._con.execute(
            f"drop table if exists l_{PREFIXES_VIEW}"
        ).execute(
            f"create table l_{PREFIXES_VIEW} as "
            "select id, len, token, pos, df "
            f"FROM l_{TOKENS_DOC_FREQ_VIEW} "
            f"where len - pos + 1 >= (len * 2 * {self._t} / (1+{self._t})) "  # indexing prefix
        ).execute(
            f"drop table if exists r_{PREFIXES_VIEW}"
        ).execute(
            f"create table r_{PREFIXES_VIEW} as "
            "select id, len, token, pos, df "
            f"FROM r_{TOKENS_DOC_FREQ_VIEW} "
            f"where len - pos + 1 >= (len * 2 * {self._t} / (1+{self._t})) "  # indexing prefix
        )

        l_widows = self._con.execute(
            "select count(*) "
            f"from l_{PREFIXES_VIEW} "
            f"where df = {self._widow_placeholder}"
        ).fetchall()[0][0]

        r_widows = self._con.execute(
            "select count(*) "
            f"from r_{PREFIXES_VIEW} "
            f"where df = {self._widow_placeholder}"
        ).fetchall()[0][0]

        self._r, self._s = ('l_', 'r_') if l_widows > r_widows else ('r_', 'l_')

        self._con.execute(
            f"drop table if exists {self._s}{PREFIXES_VIEW}"
        ).execute(
            f"create table {self._s}{PREFIXES_VIEW} as "
            "select id, len, token, pos "
            f"FROM {self._s}{TOKENS_DOC_FREQ_VIEW} "
            f"where len - pos + 1 >= (len * {self._t}) "  # probing prefix
        )

    def candidates(self):
        self._con.execute(
            f"drop table if exists {CANDIDATE_SET_VIEW}"
        ).execute(
            f"CREATE table {CANDIDATE_SET_VIEW} AS ("
            "SELECT rpfx.id AS Rid, spfx.id AS Sid "
            ", MAX(rpfx.pos) as RmaxPos, MAX(spfx.pos) as SmaxPos, count(*) as pfxOverlap "
            f"FROM {self._r}{PREFIXES_VIEW} rpfx, {self._s}{PREFIXES_VIEW} spfx "
            "WHERE rpfx.token = spfx.token "
            # length filter
            f"AND rpfx.len >= (spfx.len * {self._t})"
            f"AND spfx.len >= (rpfx.len * {self._t})"
            # positional filter
            "AND LEAST((rpfx.len - rpfx.pos + 1), (spfx.len - spfx.pos + 1)) >= "
            f"((rpfx.len + spfx.len) * {self._t} / (1 + {self._t})) "
            "GROUP BY rpfx.id, spfx.id "
            ")"
        ).execute(
            f"drop table if exists {PREFIXES_VIEW}"
        ).execute(
            f"drop table if exists {self._r}{PREFIXES_VIEW};"
            f"drop table if exists {self._s}{PREFIXES_VIEW};"
        )

    def matches(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            f"create table {self._out_table_name} as "
            f"select R.id as {self._l_out_prefix}{self._l_key_attr}, S.id as {self._r_out_prefix}{self._r_key_attr} "
            f"from {self._r}{TOKENS_DOC_FREQ_VIEW} R, {self._s}{TOKENS_DOC_FREQ_VIEW} S, {CANDIDATE_SET_VIEW} c "
            "where c.Rid = R.id "
            "and c.Sid = S.id "
            "and R.token = S.token "
            "and R.pos >= RmaxPos "
            "and S.pos >= SmaxPos "
            "group by R.id, S.id, R.len, S.len, pfxOverlap "
            f"having count(*) + pfxOverlap - 1 >= ((R.len + S.len) * {self._t} / (1+{self._t}))"
        ).execute(
            f"drop table if exists l_{TOKENS_DOC_FREQ_VIEW};"
            f"drop table if exists r_{TOKENS_DOC_FREQ_VIEW};"
        ).execute(
            f"drop table if exists {CANDIDATE_SET_VIEW}"
        )

    def matches_brute_force(self):
        self._con.execute(
            f"drop table if exists {self._out_table_name}"
        ).execute(
            f"create table {self._out_table_name} as "
            f"select L.id as {self._l_out_prefix}{self._l_key_attr}, R.id as {self._r_out_prefix}{self._r_key_attr} "
            f"from l_{TOKENS_VIEW} as L, r_{TOKENS_VIEW} as R "
            "where L.token = R.token "
            "group by L.id, L.len, R.id, R.len "
            f"having count(*) >= ((L.len + R.len) * {self._t} / (1+{self._t}))"
        ).execute(
            f"drop table if exists l_{TOKENS_VIEW};"
            f"drop table if exists r_{TOKENS_VIEW};"
        )

    def clear(self):
        self._con.execute(
            f"drop table if exists l_{INPUT_TABLE};"
            f"drop table if exists r_{INPUT_TABLE};"
            f"drop table if exists l_{TOKENS_VIEW};"
            f"drop table if exists r_{TOKENS_VIEW};"
            f"drop table if exists full_outer_{DOC_FREQ_VIEW};"
            f"drop table if exists {DOC_FREQ_VIEW};"
            f"drop table if exists l_{TOKENS_DOC_FREQ_VIEW};"
            f"drop table if exists r_{TOKENS_DOC_FREQ_VIEW};"
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
