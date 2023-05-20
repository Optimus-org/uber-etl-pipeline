package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class ProcessCleaning extends  Transformation{

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        inputDf = inputDf.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")) )
                .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")) )
                .withColumn("trip_id", monotonically_increasing_id());
        inputDf = inputDf.dropDuplicates();
        return inputDf;
    }
}
