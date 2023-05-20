package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class GenerateDropoffLocationDim extends  Transformation{

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        Dataset<Row> passengerCountDim  = inputDf.withColumn("dropoff_location_id", col("trip_id"))
                                                 .select(col("dropoff_location_id"),col("dropoff_longitude"), col("dropoff_latitude"));
        return passengerCountDim ;
    }
}
