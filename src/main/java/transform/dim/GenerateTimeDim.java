package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
public class GenerateTimeDim extends  Transformation{

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {

        Dataset<Row> datetimeDim = inputDf
                .withColumn("pick_hour", hour(col("tpep_pickup_datetime")))
                .withColumn("pick_day", dayofweek(col("tpep_pickup_datetime")))
                .withColumn("pick_month", month(col("tpep_pickup_datetime")))
                .withColumn("pick_year", month(col("tpep_pickup_datetime")))
                .withColumn("pick_weekday", month(col("tpep_pickup_datetime")))
                .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime"))
                .withColumn("drop_hour", hour(col("tpep_dropoff_datetime")))
                .withColumn("drop_day", dayofweek(col("tpep_dropoff_datetime")))
                .withColumn("drop_month", month(col("tpep_dropoff_datetime")))
                .withColumn("drop_year", month(col("tpep_dropoff_datetime")))
                .withColumn("drop_weekday", month(col("tpep_dropoff_datetime")))
                .withColumn("datetime_id", col("trip_id"))
                .select("datetime_id","tpep_pickup_datetime","tpep_pickup_datetime", "pick_hour",  "pick_day", "pick_month", "pick_year", "pick_weekday", "drop_hour"
                , "drop_day",  "drop_day", "drop_month", "drop_year", "drop_weekday");
                ;

        return datetimeDim;
    }
}
