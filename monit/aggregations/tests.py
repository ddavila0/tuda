from .agg_utils import agg_wrapper

@agg_wrapper(__file__, group_name="tests")
def naive_test(df, chunked=False):
    return
