from .agg_utils import agg_wrapper

@agg_wrapper(source_names="test")
def shape_test(df):
    return "you gave me a df this big: {}".format(df.shape)

@agg_wrapper(source_names="test")
def column_test(df):
    return "columns: {}".format((", ").join(df.columns))
