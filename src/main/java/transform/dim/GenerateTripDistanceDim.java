package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class GenerateTripDistanceDim extends  Transformation{

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        Dataset<Row> tripDistanceDim  = inputDf.withColumn("trip_distance", col("passenger_count"))
                .withColumn("trip_distance_id", col("trip_id"))
                .select("trip_distance_id", "trip_distance", "passenger_count");
        return tripDistanceDim ;
    }
}
