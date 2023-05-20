package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class GeneratePickupLocationDim extends  Transformation{

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        Dataset<Row> passengerCountDim  = inputDf
                .withColumn("pickup_location_id", col("trip_id"))
                .select(col("pickup_location_id"),col("pickup_latitude"), col("pickup_longitude"));
        return passengerCountDim ;
    }
}
