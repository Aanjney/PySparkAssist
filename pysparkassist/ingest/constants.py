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

# ponytail: minimal allowlist; expand when evals show missed methods
PYSPARK_METHODS = {
    "select", "filter", "join", "groupBy", "groupby", "orderBy", "withColumn",
    "drop", "cache", "persist", "unpersist", "explain", "createDataFrame",
    "parquet", "csv", "json", "orc", "jdbc", "fit", "transform", "evaluate",
    "where", "distinct", "limit", "collect", "count", "agg", "alias", "union",
    "repartition", "coalesce", "fillna", "dropna", "withColumnRenamed",
    "createOrReplaceTempView", "registerTempTable", "write", "read", "load", "save",
    "writeStream", "readStream", "watermark", "outputMode", "trigger",
    "awaitTermination", "withWatermark",
}

PYTHON_BUILTINS = {
    "print", "len", "str", "int", "float", "list", "dict", "set", "tuple", "range",
    "enumerate", "zip", "map", "sorted", "open", "type", "isinstance", "getattr",
    "setattr", "hasattr", "super", "format", "repr", "abs", "min", "max", "sum",
    "any", "all", "iter", "next", "input", "vars", "dir", "id", "hash", "round",
    "append", "extend", "pop", "get", "set", "add", "run",
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
