package transform.dim;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class GenerateRateCodeDim extends  Transformation {

    public  GenerateRateCodeDim(SparkSession spark){
        this.spark = spark;
    }
    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        Dataset<Row> raterateCodeDim   = inputDf
                .withColumn("rate_code_id", col("trip_id"))
                .select( col("RatecodeID"), col("rate_code_id"));

        Dataset<Row>  rateCodeTypeRef = createRateCodeRef();
        rateCodeTypeRef.printSchema();
        raterateCodeDim = raterateCodeDim.join(rateCodeTypeRef, raterateCodeDim.col("RatecodeID").equalTo(rateCodeTypeRef.col("rate_code_id_tmp")), "inner");
        raterateCodeDim.printSchema();
        raterateCodeDim =  raterateCodeDim.select( col("RatecodeID"), col("rate_code_id"), col("rate_description"));
        // Convert the map to a Dataset<Row>
        raterateCodeDim.printSchema();
        return raterateCodeDim ;
    }
    public  Dataset<Row> createRateCodeRef(){
        Dataset<Row> rateCode;

        Map<Integer, String> rateCodeType = new HashMap<>();
        rateCodeType.put(1, "Standard rate");
        rateCodeType.put(2, "JFK");
        rateCodeType.put(3, "Newark");
        rateCodeType.put(4, "Nassau or Westchester");
        rateCodeType.put(5, "Negotiated fare");
        rateCodeType.put(6, "Group ride");

        List<Row> rows = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : rateCodeType.entrySet()) {
            Row row = RowFactory.create(entry.getKey(), entry.getValue());
            rows.add(row);
        }
        // Create the schema for the DataFrame
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("rate_code_id_tmp", DataTypes.IntegerType, false),
                DataTypes.createStructField("rate_description", DataTypes.StringType, false)
        });
        // Create the DataFrame
        Dataset<Row> dfRateCodeType = spark.createDataFrame(rows, schema);
        return dfRateCodeType;
    }
}
