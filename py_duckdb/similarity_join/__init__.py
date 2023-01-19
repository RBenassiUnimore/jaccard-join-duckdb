import duckdb


# To deprecate once the generic join has been implemented
def jaccard_self_join(
        con: duckdb.DuckDBPyConnection):
    return con


def jaccard_join(
        con: duckdb.DuckDBPyConnection
):
    return con


# For testing and benchmarking purposes only
def jaccard_join_brute_force(
        con: duckdb.DuckDBPyConnection):
    return con
