from .agg_utils import agg_wrapper

@agg_wrapper(__file__, group_name="es_tests")
def shape_test(df, chunked=False):
    """Verify DataFrame is propery communicated to aggregators"""
    return "you gave me a df this big: {}".format(df.shape)

@agg_wrapper(__file__, group_name="es_tests")
def column_test(df, chunked=False):
    """Verify DataFrame is propery communicated to aggregators"""
    return "columns: {}".format((", ").join(df.columns))
