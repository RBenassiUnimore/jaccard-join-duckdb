from py_duckdb.similarity_join.join.jaccard_join import *
from py_duckdb.similarity_join.tokenizers import *


def evaluate(
        con: duckdb.DuckDBPyConnection,
        ground_truth_table: str,
        similarity_join_table: str,
        gt_l_key='l_id',
        gt_r_key='r_id',
        sj_l_key='l_id',
        sj_r_key='r_id'
):
    con.execute("drop view if exists confusion_mtx").execute(
        "create view confusion_mtx as "
        f"select gt.{gt_l_key} as gtk1, gt.{gt_r_key} as gtk2, "
        f"sj.{sj_l_key} as sjk1, sj.{sj_r_key} as sjk2 "
        f"from {ground_truth_table} gt "
        f"full outer join {similarity_join_table} sj "
        f"on (gt.{gt_l_key} = sj.{sj_l_key} and gt.{gt_r_key} = sj.{sj_r_key}) "
        f"or (gt.{gt_l_key} = sj.{sj_r_key} and gt.{gt_r_key} = sj.{sj_l_key})"
    )

    tp = con.execute(
        "select count(*) "
        "from confusion_mtx "
        "where sjk1 is not null "
        "and sjk2 is not null "
    ).fetchall()[0][0]

    fp = con.execute(
        "select count(*) "
        "from confusion_mtx "
        "where gtk1 is null "
    ).fetchall()[0][0]

    fn = con.execute(
        "select count(*) "
        "from confusion_mtx "
        "where sjk1 is null "
    ).fetchall()[0][0]

    con.execute("drop view confusion_mtx")

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
