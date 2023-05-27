import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.*;

public class ProcessingTest {

    private static SparkSession spark;

    @BeforeClass
    public static void setUp() {
        spark = SparkSession.builder()
                .appName("MergeDataFramesTest")
                .master("local")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        spark.stop();
    }

    @Test
    public void testMergeDataFrames() {
        // Create the input DataFrames
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        });

        Row[] rows1 = new Row[]{
                RowFactory.create(1, "John"),
                RowFactory.create(2, "Jane"),
                RowFactory.create(3, "Bob")
        };
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(rows1), schema);

        Row[] rows2 = new Row[]{
                RowFactory.create(1, "Doe"),
                RowFactory.create(2, "Doe"),
                RowFactory.create(4, "Smith")
        };
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(rows2), schema);

        // Expected merged DataFrame
        Row[] expectedRows = new Row[]{
                RowFactory.create(1, "John", "Doe"),
                RowFactory.create(2, "Jane", "Doe"),
                RowFactory.create(3, "Bob", null),
                RowFactory.create(4, null, "Smith")
        };
        Dataset<Row> expectedDF = spark.createDataFrame(Arrays.asList(expectedRows), schema.add("last_name", DataTypes.StringType, true));

        // Perform the merge operation
        Dataset<Row> mergedDF = df1.join(df2, "id", "outer")
                .select(df1.col("id"), df1.col("name"), df2.col("name").alias("last_name"));

        // Compare the actual merged DataFrame with the expected DataFrame
        assertEquals(expectedDF.collectAsList(), mergedDF.collectAsList());
    }
}
