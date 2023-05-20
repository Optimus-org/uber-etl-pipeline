package uber.processing;


import java.util.ArrayList;


import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaSparkContext;
import service.Processing;

public class DataLoader {

	public static void main(String[] args) {
		String filePath = args[0] ;
		System.out.println("File "+filePath);
		SparkSession spark = SparkSession.builder()
				.appName("MainProcessFile")
				.master("local[*]")
				.getOrCreate() ;

		Dataset<Row>  df = spark
				.read()
				.format("csv")
				.option("header","true")
				.option("inferSchema","true")
				.load(filePath);
		df.printSchema();
		System.out.println("Initial df ") ;
		df.show(10);


		System.out.println("le nombre de partition est "+df.rdd().getNumPartitions());
		System.out.println("la taille est "+df.count());
		Processing processing = new Processing (df,spark);
		processing.process();
	}

}

