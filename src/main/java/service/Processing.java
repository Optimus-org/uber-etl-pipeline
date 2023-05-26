package service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import java.time.LocalDate;

import java.time.format.DateTimeFormatter;

import transform.dim.*;
import uber.processing.constant.UberConstant;

public class Processing  {
    Dataset<Row> inputDataframe;
    SparkSession spark;
    public Processing(Dataset<Row> inputDataframe, SparkSession spark){
        this.inputDataframe = inputDataframe;
        this.spark =spark;
    }
    public void process() {

        ProcessCleaning  generateDim = new ProcessCleaning();

        Dataset<Row> dataframeCleared = generateDim.transform(inputDataframe);
        Dataset<Row> datetimeDim  = (new  GenerateTimeDim() ).transform(dataframeCleared);
        Dataset<Row> passengerCountDim  = (new GeneratePassengerCountDim() ).transform(dataframeCleared);
        Dataset<Row> pickupLocationDim  = (new GeneratePickupLocationDim() ).transform(dataframeCleared);
        Dataset<Row> dropOffLocationDim  = (new GenerateDropoffLocationDim() ).transform(dataframeCleared);
        Dataset<Row> rateCodeDim  = (new  GenerateRateCodeDim(spark) ).transform(dataframeCleared);
        Dataset<Row> tripDistanceDim  = (new  GenerateTripDistanceDim() ).transform(dataframeCleared);
        Dataset<Row> rateTripDim  = (new  GenerateTimeDim() ).transform(dataframeCleared);
        Dataset<Row> paymentTypeDim  = (new  GeneratePaymentTypeDim(spark) ).transform(dataframeCleared);

        //Generate fact Table
        Dataset<Row> mergeData  = dataframeCleared.join(passengerCountDim, dataframeCleared.col("trip_id").equalTo(passengerCountDim.col("passenger_count_id")),"left")
                .join(tripDistanceDim, dataframeCleared.col("trip_id").equalTo(tripDistanceDim.col("trip_distance_id")) ,"left")
                .join(rateCodeDim,dataframeCleared.col("trip_id").equalTo(rateCodeDim.col("rate_code_id")) ,"left")
                .join(pickupLocationDim, dataframeCleared.col("trip_id").equalTo(pickupLocationDim.col("pickup_location_id")))
                .join(dropOffLocationDim, dataframeCleared.col("trip_id").equalTo(dropOffLocationDim.col("dropoff_location_id")))
                .join(datetimeDim, dataframeCleared.col("trip_id").equalTo(datetimeDim.col("datetime_id")))
               .join(paymentTypeDim, dataframeCleared.col("trip_id").equalTo(paymentTypeDim.col("payment_type_id")))
                .select("trip_id","VendorID", "datetime_id", "passenger_count_id",
                "trip_distance_id", "rate_code_id", "store_and_fwd_flag", "pickup_location_id", "dropoff_location_id",
                "payment_type_id", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                "improvement_surcharge", "total_amount");
        System.out.println("Test  ici"+mergeData.count());
        mergeData.printSchema();
        // Save
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate currentDate = LocalDate.now();
        
        String dateTime = currentDate.format(formatter);
        mergeData.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_FACT_TABLE+"/partition=%s", dateTime));
        datetimeDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_DATETIME_DIM+"/partition=%s", dateTime));
        pickupLocationDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_PICKUPLOCATION_DIM+"/partition=%s", dateTime));
        dropOffLocationDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_DROPOFFLOCATION_DIM+"/partition=%s", dateTime));
        rateCodeDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_RATECODE_DIM+"/partition=%s", dateTime));
        tripDistanceDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_TRIPDISTANCE_DIM+"/partition=%s", dateTime));
        rateTripDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_RATECODE_DIM+"/partition=%s", dateTime));
        paymentTypeDim.write().mode("overwrite").save(String.format(UberConstant.HDFS_UBER_PATH+"/"+UberConstant.HDFS_PAYMENTTYPE_DIM+"/partition=%s", dateTime));
    }
}
