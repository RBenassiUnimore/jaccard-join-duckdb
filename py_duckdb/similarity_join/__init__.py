import duckdb
import string
from py_duckdb.similarity_join import tokenizers

# To deprecate once the generic join has been implemented
#def jaccard_self_join(
#        con: duckdb.DuckDBPyConnection):
#    return con

def jaccard_join(
        con: duckdb.DuckDBPyConnection,
        l_table: string,
        r_table: string,
        l_key_attr: string,
        r_key_attr: string,
        l_join_attr: string,
        r_join_attr: string,
        tokenizer: tokenizers.Tokenizer,
        threshold: float,
        out_table_name: string):
    global __con, __l_table, __r_table, __l_key_attr, __r_key_attr, __l_join_attr, __r_join_attr, __t, __tokenizer, __out_table_name, __tokzr_query
    __con = con
    __l_table = l_table
    __r_table = r_table
    __l_key_attr = l_key_attr
    __r_key_attr = r_key_attr
    __l_join_attr = l_join_attr
    __r_join_attr = r_join_attr
    __t = threshold
    __tokenizer = tokenizer
    __out_table_name = out_table_name

    __tokzr_query = __get_query_from_tokenizer(__tokenizer)

    if __l_table:
        if( __l_table == __r_table or not __r_table):
            __jaccard_self_join()
        else:
            __jaccard_join()
    return con


#region Jaccard Self Join Functions
def __jaccard_self_join():
    __create_unique_view_self(__con,__l_table,__l_key_attr,__l_join_attr)
    __generate_token_view(__con,__tokzr_query)
    __token_doc_frequency_self()
    __create_prefixes_view_self()
    __matches_self()

#region Document Frequency
def __token_doc_frequency_self():
    __create_view_doc_frequency_self()
    __create_view_token_df_self()

def __create_view_doc_frequency_self():
    __con.execute("drop view if exists df").execute(
        "CREATE VIEW df AS "
        "SELECT token, count(*) AS df "
        "FROM tokens "
        "GROUP BY token "
        # ORDER BY later!
    )
def __create_view_token_df_self():
    __con.execute("drop view if exists tkdf").execute(
        "CREATE VIEW tkdf AS "
        "select rid, rlen, tokens.token "
        ", row_number() OVER (PARTITION BY rid ORDER BY df, tokens.token) as pos "
        "from tokens, df "
        "where tokens.token = df.token"
    )

#endregion

def __create_prefixes_view_self():
    __con.execute("drop view if exists prefixes").execute(
        "create view prefixes as "
        "select rid, rlen, token, pos "
        "from tkdf "
        f"where rlen - pos + 1 >= ceil(rlen * {__t}) "
    )

#region Matches

def __matches_self():
    __create_view_candidate_set_self()
    __create_table_matches_self()

def __create_view_candidate_set_self():
    __con.execute("drop view if exists candset").execute(
        "CREATE VIEW candset AS ("
        "SELECT pr1.rid AS rid1, pr2.rid AS rid2 "
        ", MAX(pr1.pos) as maxPos1, MAX(pr2.pos) as maxPos2, count(*) as prOverlap "
        "FROM prefixes pr1, prefixes pr2 "
        "WHERE pr1.rid < pr2.rid "
        "AND pr1.token = pr2.token "
        # length filter
        f"AND pr1.rlen >= ceil({__t} * pr2.rlen)"
        # prefix filter
        f"AND pr1.rlen - pr1.pos + 1 >= CEIL(pr1.rlen * 2 * {__t} / (1+{__t})) "
        # positional filter
        "AND LEAST((pr1.rlen - pr1.pos + 1), (pr2.rlen - pr2.pos + 1)) >= "
        f"CEIL((pr1.rlen + pr2.rlen) * {__t} / (1 + {__t})) "
        "GROUP BY pr1.rid, pr2.rid "
        ")"
    )

def __create_table_matches_self():
    __con.execute(f"drop view if exists {__out_table_name}").execute(
        f"create view {__out_table_name} as "
        "select r1.rid as rid1, r2.rid as rid2 "
        "from tkdf r1, tkdf r2, candset c "
        "where c.rid1 = r1.rid "
        "and c.rid2 = r2.rid "
        "and r1.token = r2.token "
        "and r1.pos > maxPos1 "
        "and r2.pos > maxPos2 "
        "group by r1.rid, r2.rid, r1.rlen, r2.rlen, prOverlap "
        f"having count(*) + prOverlap >= (r1.rlen + r2.rlen) * {__t} / (1+{__t})"
    )

#endregion

#endregion

#region Jaccard Join Functions

def __jaccard_join():
    __create_unique_view_general(__con,__l_table,__l_key_attr,__l_join_attr,__r_table,__r_key_attr,__r_join_attr)
    __generate_token_view(__con,__tokzr_query)
    __token_doc_frequency_general()
    __create_prefixes_view_general()
    __matches_general()

#region Document Frequency
def __token_doc_frequency_general():
    __create_view_full_outer_join_general()
    __create_view_doc_frequency_general()
    __create_view_token_df_general()

def __create_view_full_outer_join_general():
    __con.execute("drop view if exists full_outer_df").execute(
        "create view full_outer_df as "
        "select s1.token as tk1, s1.df as df1, s2.token as tk2, s2.df as df2 "
        "from ("
        "SELECT token, count(*) AS df "
        "FROM tokens "
        f"where src = '{__l_table}' "
        "GROUP BY token "
        ") as s1 "
        "full outer join ("
        "SELECT token, count(*) AS df "
        "FROM tokens "
        f"where src = '{__r_table}' "
        "GROUP BY token "
        ") as s2 "
        "on s1.token = s2.token"
    )

def __create_view_doc_frequency_general():
    __con.execute("drop view if exists df").execute(
        "create view df as "
        "select tk1 as token, df1 * df2 as df "
        "from full_outer_df "
        "where tk1 is not null "
        "and tk2 is not null"
    )

def __create_view_token_df_general():
    __con.execute("drop view if exists tkdf").execute(
        "create view tkdf as "
        "select tokens.* "
        ", row_number() OVER (PARTITION BY rid ORDER BY df, tokens.token) as pos "
        "from tokens "
        "join df on tokens.token = df.token"
    )

#endregion

def __create_prefixes_view_general() :
    widows1 = __con.execute(
        "select count(*) "
        "from full_outer_df "
        "where tk2 is null"
    ).fetchall()[0][0]
    widows2 = __con.execute(
        "select count(*) "
        "from full_outer_df "
        "where tk1 is null"
    ).fetchall()[0][0]
    R, S = (__l_table, __r_table) if widows1 > widows2 else (__r_table, __l_table)

    __con.execute("drop view if exists prefixes").execute(
        "create view prefixes as "
        "select src, rid, rlen, token, pos "
        "from ("
        "SELECT * "
        "FROM tkdf "
        f"where src = '{R}' "
        f"and rlen - pos + 1 >= ceil(rlen * {__t}) "
        ") union ("
        "SELECT * "
        "FROM tkdf "
        f"where src = '{S}' "
        f"and rlen - pos + 1 >= ceil(rlen * 2 * {__t} / (1+{__t})) "
        ")"
    )

#region Matches

def __matches_general():
    __create_view_candidate_set_general()
    __create_table_matches_general()

def __create_view_candidate_set_general():
    __con.execute("drop view if exists candset").execute(
        "CREATE VIEW candset AS ("
        "SELECT pr1.rid AS rid1, pr2.rid AS rid2 "
        ", MAX(pr1.pos) as maxPos1, MAX(pr2.pos) as maxPos2, count(*) as prOverlap "
        "FROM prefixes pr1, prefixes pr2 "
        "WHERE pr1.token = pr2.token "
        f"and pr1.src = '{__l_table}' "
        f"and pr2.src = '{__r_table}'"
        # length filter
        f"AND pr1.rlen >= ceil({__t} * pr2.rlen)"
        # positional filter
        "AND LEAST((pr1.rlen - pr1.pos + 1), (pr2.rlen - pr2.pos + 1)) >= "
        f"CEIL((pr1.rlen + pr2.rlen) * {__t} / (1 + {__t})) "
        "GROUP BY pr1.rid, pr2.rid "
        ")"
    )

def __create_table_matches_general():
    __con.execute(f"drop view if exists {__out_table_name}").execute(
        f"create view {__out_table_name} as "
        "select r1.rid as rid1, r2.rid as rid2 "
        "from tkdf r1, tkdf r2, candset c "
        "where c.rid1 = r1.rid "
        "and c.rid2 = r2.rid "
        "and r1.token = r2.token "
        "and r1.pos > maxPos1 "
        "and r2.pos > maxPos2 "
        "group by r1.rid, r2.rid, r1.rlen, r2.rlen, prOverlap "
        f"having count(*) + prOverlap >= (r1.rlen + r2.rlen) * {__t} / (1+{__t})"
    )

#endregion

#endregion

#region Tokenization Functions
def __get_query_from_tokenizer(tokenizer: tokenizers.Tokenizer):
    tokzr_query = ''
    if isinstance(tokenizer, tokenizers.WordsTokzr):
        separators = tokenizer.get_info()
        tokzr_query = (f"select src, rid, len(tks) as rlen, lower(unnest(tks)) as token "
                       "from ( "
                       f"select distinct src, rid, str_split_regex(val, {separators}) as tks """
                       f"from input "
                       ") ")
    elif isinstance(tokenizer, tokenizers.QGramsTokzr):
        q = tokenizer.get_info()
        tokzr_query = (f"select distinct src, rid, rlen "
                       f", substring(concat(repeat('#', {q} - 1), "
                       f"lower(val), "
                       f"repeat('#',{q} - 1)),"
                       f"x, {q}) as token "
                       "from ("
                       f"select *, len(val) + {q} - 1 as rlen, unnest(generate_series(1, rlen)) as x "
                       f"from input "
                       ")")
    return tokzr_query


def __generate_token_view(
        con: duckdb.DuckDBPyConnection,
        tokzr_query: string):
    con.execute("drop view if exists tokens").execute(
        "create view tokens as " + tokzr_query
    )

#endregion

#region Creation Unique View

def __create_unique_view_self(
        con: duckdb.DuckDBPyConnection,
        l_table: string,
        l_key_attr: string,
        l_join_attr: string):
    con.execute("drop view if exists input")
    con.execute(
        "create view input as "
        f"select '{l_table}' as src, {l_key_attr} as rid, {l_join_attr} as val "
        f"from '{l_table}' "
    )

def __create_unique_view_general(
        con: duckdb.DuckDBPyConnection,
        l_table: string,
        l_key_attr: string,
        l_join_attr: string,
        r_table: string,
        r_key_attr: string,
        r_join_attr: string
):
    con.execute("drop view if exists input")
    con.execute(
        "create view input as "
        f"select '{l_table}' as src, {l_key_attr} as rid, {l_join_attr} as val "
        f"from '{l_table}' "
        "union "
        f"select '{r_table}' as src, {r_key_attr} as rid, {r_join_attr} as val "
        f"from '{r_table}' "
    )

#endregion


# For testing and benchmarking purposes only
def jaccard_join_brute_force(
        con_bf: duckdb.DuckDBPyConnection,
        l_table_bf: string,
        r_table_bf: string,
        l_key_attr_bf: string,
        r_key_attr_bf: string,
        l_join_attr_bf: string,
        r_join_attr_bf: string,
        tokenizer_bf: tokenizers.Tokenizer,
        threshold_bf: float):

    global __con_bf, __l_table_bf, __r_table_bf, __l_key_attr_bf, __r_key_attr_bf, __l_join_attr_bf, __r_join_attr_bf,  __t_bf, __tokzr_query_bf, __tokenizer_bf

    __con_bf = con_bf
    __l_table_bf = l_table_bf
    __r_table_bf = r_table_bf
    __l_key_attr_bf = l_key_attr_bf
    __r_key_attr_bf = r_key_attr_bf
    __l_join_attr_bf = l_join_attr_bf
    __r_join_attr_bf = r_join_attr_bf
    __tokenizer_bf = tokenizer_bf
    __t_bf = threshold_bf

    __tokzr_query_bf = __get_query_from_tokenizer(__tokenizer_bf)

    if __l_table_bf:
        if( __l_table_bf == __r_table_bf or not __r_table_bf):
            __jaccard_join_self_brute_force()
        else:
            __jaccard_join_general_brute_force()

    return con_bf



def __jaccard_join_self_brute_force():

    __create_unique_view_self(__con_bf,__l_table_bf,__l_key_attr_bf,__l_join_attr_bf)
    __generate_token_view(__con_bf,__tokzr_query_bf)
    __create_view_brute_force_self()

def __create_view_brute_force_self():
    __con_bf.execute("drop view if exists bfjoin").execute(
        "create view bfjoin as "
        "select r1.rid as rid1, r2.rid as rid2, count(*) as overlap "
        "from tokens as r1, tokens as r2 "
        "where r1.token = r2.token "
        "and r1.rid < r2.rid "
        "group by r1.rid, r1.rlen, r2.rid, r2.rlen "
        f"having count(*) >= ceil({__t_bf} / (1+{__t_bf}) * (r1.rlen + r2.rlen))"
    )

def __jaccard_join_general_brute_force():
    __create_unique_view_general(__con_bf,__l_table_bf,__l_key_attr_bf,__l_join_attr_bf,__r_table_bf, __r_key_attr_bf, __r_join_attr_bf)
    __generate_token_view(__con_bf,__tokzr_query_bf)
    __create_view_brute_force_general()

def __create_view_brute_force_general():
    __con_bf.execute("drop view if exists bfjoin").execute(
        "create view bfjoin as "
        "select r1.rid as rid1, r2.rid as rid2, count(*) as overlap "
        "from tokens as r1, tokens as r2 "
        "where r1.token = r2.token "
        # "and r1.rid < r2.rid "
        f"and r1.src = '{__l_table_bf}' and r2.src = '{__r_table_bf}' "
        "group by r1.rid, r1.rlen, r2.rid, r2.rlen "
        f"having count(*) >= ceil({__t_bf} / (1+{__t_bf}) * (r1.rlen + r2.rlen))"
    )