package uber.processing.constant;

import org.joda.time.DateTime;

public class UberConstant {

    public static final String HDFS_UBER_PATH= "hdfs://192.168.1.13:8020//data/dev_source/uber";
    public static final String HDFS_FACT_TABLE ="fact_table";
    public static final String HDFS_DATETIME_DIM = "datetimeDim";
    public static final String HDFS_PICKUPLOCATION_DIM ="pickupLocationDim";
    public static final String HDFS_DROPOFFLOCATION_DIM = "dropOffLocationDim";
    public static final String HDFS_RATECODE_DIM = "rateCodeDim";

    public static final String HDFS_RATETRIP_DIM = "rateTripDim";
    public static final String HDFS_TRIPDISTANCE_DIM = "tripDistanceDim";
    public static final String HDFS_PAYMENTTYPE_DIM = "paymentTypeDim";

}
