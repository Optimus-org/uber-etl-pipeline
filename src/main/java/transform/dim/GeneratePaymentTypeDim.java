package transform.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class GeneratePaymentTypeDim extends  Transformation {

    public  GeneratePaymentTypeDim(SparkSession sparkSession) {
        this.spark = sparkSession;
    }
    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        Dataset<Row> paymentTypeeDim   = inputDf
                .withColumn("payment_type_id", col("trip_id"))
                .select( "payment_type_id", "payment_type");

        Dataset<Row>  rateCodeTypeRef = createRateCodeRef();
        rateCodeTypeRef = paymentTypeeDim.join(rateCodeTypeRef, paymentTypeeDim.col("payment_type").equalTo(rateCodeTypeRef.col("payment_type_tmp")), "inner")
                .select("payment_type_id", "payment_type",  "payment_type_name");
        // Convert the map to a Dataset<Row>
        return rateCodeTypeRef ;
    }
    public  Dataset<Row> createRateCodeRef(){
        Dataset<Row> rateCode;

        Map<Integer, String> rateCodeType = new HashMap<>();
        rateCodeType.put(1, "Credit card");
        rateCodeType.put(2, "Cash");
        rateCodeType.put(3, "No charge");
        rateCodeType.put(4, "Dispute");
        rateCodeType.put(5, "Unknown");
        rateCodeType.put(6, "Voided trip");

        List<Row> rows = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : rateCodeType.entrySet()) {
            Row row = RowFactory.create(entry.getKey(), entry.getValue());
            rows.add(row);
        }
        // Create the schema for the DataFrame
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("payment_type_tmp", DataTypes.IntegerType, false),
                DataTypes.createStructField("payment_type_name", DataTypes.StringType, false)
        });
        // Create the DataFrame
        Dataset<Row> dfRateCodeType = spark.createDataFrame(rows, schema);
        return dfRateCodeType;
    }
}
