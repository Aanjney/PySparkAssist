PYSPARK_CLASSES = {
    "SparkSession", "DataFrame", "Column", "Row", "GroupedData",
    "DataFrameReader", "DataFrameWriter", "SparkContext", "RDD",
    "StreamingQuery", "Window", "WindowSpec", "DataStreamReader",
    "DataStreamWriter", "Catalog", "UDFRegistration",
    "Pipeline", "Estimator", "Transformer", "Evaluator",
    "CrossValidator", "TrainValidationSplit",
}

PYSPARK_MODULES = {
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.window", "pyspark.ml", "pyspark.ml.feature",
    "pyspark.ml.classification", "pyspark.ml.regression",
    "pyspark.ml.clustering", "pyspark.ml.pipeline",
    "pyspark.streaming", "pyspark.pandas", "pyspark.sql.streaming",
}

DOMAIN_TERMS = {
    "ingestion", "ingest", "etl", "pipeline", "partition", "partitioning",
    "repartition", "coalesce", "broadcast", "shuffle", "cache", "persist",
    "unpersist", "schema", "parquet", "avro", "orc", "csv", "json",
    "jdbc", "hive", "delta", "iceberg", "catalyst", "tungsten",
    "udf", "udaf", "udtf", "aggregate", "aggregation", "groupby",
    "join", "crossjoin", "filter", "select", "withcolumn",
    "mappartitions", "foreachpartition", "collect", "take", "show",
    "explain", "checkpoint", "bucketing", "skew", "spill",
    "executor", "driver", "cluster", "yarn", "mesos", "kubernetes",
    "spark submit", "sparksubmit", "spark-submit", "sparksession",
    "dataframe", "dataset", "rdd", "resilient distributed",
    "lazy evaluation", "transformation", "action", "dag",
    "serialization", "deserialization", "kryo", "arrow",
    "vectorized", "pandas udf", "window function",
    "structured streaming", "dstream", "watermark", "trigger",
    "read", "write", "load", "save", "format", "option",
    "sql", "createtempview", "createglobaltempview",
    "ml", "mllib", "feature engineering", "model", "fit", "transform",
}
