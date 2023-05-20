package transform.dim;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public abstract class  Transformation {
  SparkSession spark;
  public abstract Dataset<Row> transform(Dataset<Row>  inputDf);

}
