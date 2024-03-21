from pyspark.sql import SparkSession 

ARTIFACTS_LIST = ['io.delta:delta-spark_2.12:3.1.0',
                  'io.delta:delta-flink:3.1.0',
]


def create_local_cluster(ARTIFACTS=None):

    if not ARTIFACTS:
        ARTIFACTS = ARTIFACTS_LIST

    packages = ",".join(ARTIFACTS)

    spark = (SparkSession.builder.appName('my_awesome')
             .config('spark.jars.packages', packages)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate()
    )
    
    return spark 

def create_multinode_cluster():
    if not ARTIFACTS:
        ARTIFACTS = ARTIFACTS_LIST

    packages = ",".join(ARTIFACTS)

    spark = (SparkSession.builder.appName('my_awesome')
             .config('spark.jars.packages', packages)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate()
    )
    
    return spark 