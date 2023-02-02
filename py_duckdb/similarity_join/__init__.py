from py_duckdb.similarity_join.join.jaccard_join import *
from py_duckdb.similarity_join.tokenizers import *


def evaluate(
        con: duckdb.DuckDBPyConnection,
        ground_truth_table: string,
        similarity_join_table: string,
        gt_l_key='rid1',
        gt_r_key='rid2',
        sj_l_key='rid1',
        sj_r_key='rid2'
):
    con.execute("drop view if exists contingency_table").execute(
        "create view contingency_table as "
        f"select gt.{gt_l_key} as gtk1, gt.{gt_r_key} as gtk2, "
        f"sj.{sj_l_key} as sjk1, sj.{sj_r_key} as sjk2 "
        f"from {ground_truth_table} gt "
        f"full outer join {similarity_join_table} sj "
        f"on gt.{gt_l_key} = sj.{sj_l_key} and gt.{gt_r_key} = sj.{sj_r_key} "
    )

    tp = con.execute(
        "select count(*) "
        "from contingency_table "
        "where sjk1 is not null "
        "and sjk2 is not null "
    ).fetchall()[0][0]

    fp = con.execute(
        "select count(*) "
        "from contingency_table "
        "where gtk1 is null "
    ).fetchall()[0][0]

    fn = con.execute(
        "select count(*) "
        "from contingency_table "
        "where sjk1 is null "
    ).fetchall()[0][0]

    con.execute("drop view contingency_table")

    pr = 0
    rc = 0
    fm = 0

    if tp > 0:
        pr = tp / (tp + fp)
        rc = tp / (tp + fn)
        fm = 2 * pr * rc / (pr + rc)

    return {
        'tp': tp,
        'fp': fp,
        'fn': fn,
        'pr': pr,
        'rc': rc,
        'fm': fm
    }
