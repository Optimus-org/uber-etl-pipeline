package transform.fact;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import transform.dim.Transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class GenerateFact extends Transformation {

    @Override
    public Dataset<Row> transform(Dataset<Row> inputDf) {
        return null;
    }

    public Dataset<Row> factTable(Dataset<Row> inputDf) {
        return null;
    }

}
