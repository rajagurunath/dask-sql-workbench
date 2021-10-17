from dask_sql import Context, java
from dask_sql.datacontainer import SchemaContainer


def get_dask_sql_context():
    # from dask.distributed import Client
    # client = Client()
    # print where it gets the class from. That should be the DaskSQL.jar
    print(
        java.org.codehaus.commons.compiler.CompilerFactoryFactory.class_.getProtectionDomain()
        .getCodeSource()
        .getLocation()
    )
    print(
        java.org.codehaus.commons.compiler.CompilerFactoryFactory.getDefaultCompilerFactory()
    )

    # print the JVM path, that should be your java installation
    print(java.jvmpath)

    return Context()
