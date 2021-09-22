
udf_help_text = "To more about how to define Python UDF, \
    please have a look at [dask-sql-docs](https://dask-sql.readthedocs.io/en/latest/pages/custom.html)"

def register_udf(completefn,sql_context):
    cfn = compile(completefn,'kernel','exec')
    eval(cfn,{"c":sql_context})