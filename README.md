In this repository are some examples of Spark processing in Scala, Python, and SQL.

For many typical analytical processing (OLAP) for business intelligence, where queries can be elegantly expressed in SQL, try to use Spark SQL and the Dataset API to take advantage of Spark SQL's optimized execution engine.  Also consider not using Spark, as modern data warehouses like Redshift, Databricks and Snowflake are even more optimized for SQL on columnar stores than Spark.

When the operation is not SQL-like, the old RDD construct can be simpler and faster.

Structured Streaming allows elegant expression of streaming analytics.  Spark Streaming is the legacy API and is no longer updated.

Spark's Machine Learning (ML) libraries are much less popular now in comparison to scikit-learn, Pytorch and Tensorflow.






