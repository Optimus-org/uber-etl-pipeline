spark-submit  .\target\testLearningSpark-0.0.1-SNAPSHOT.jar D:/test/
spark-submit   D:/test/


spark-submit --class uber.processing.DataLoader   --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  --driver-memory 500G  --executor-memory 500G  .\target\testLearningSpark-0.0.1-SNAPSHOT.jar  C:\Users\theProcess\Documents\Workspace\BigData\datasets\bigData_lab.csv  > Workspace\BigData\logs\Log_spark__%date%-%TIME%.txt


spark-submit --class uber.processing.DataLoader .\target\testLearningSpark-0.0.1-SNAPSHOT.jar

"D:\Workspace\BigData\datasets\searches.csv"