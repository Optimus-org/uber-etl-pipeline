package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class GeneratePassengerCountDim extends  Transformation{

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        Dataset<Row> passengerCountDim  = inputDf.withColumn("passenger_count", col("passenger_count"))
                .withColumn("passenger_count_id", col("trip_id"))
                .select("passenger_count_id","passenger_count");
        return passengerCountDim ;
    }
}
