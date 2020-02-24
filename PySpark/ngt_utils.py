import pyspark.sql.functions


def get_sum_grouped_by_date(sales: pyspark.sql.functions.DataFrame, gbdestination: str, total_destination: str,
                            source_date_field: str, format: str, agg_field: str) -> pyspark.sql.functions.DataFrame:
    """

    :type sales: object
    """
    res = sales.withColumn(gbdestination,
                           pyspark.sql.functions.year(pyspark.sql.functions.to_timestamp(source_date_field, format)))
    res = res.groupBy(gbdestination).agg({agg_field: 'sum'})
    res = res.withColumnRenamed('sum(' + agg_field + ')', total_destination).sort(gbdestination)
    return res
