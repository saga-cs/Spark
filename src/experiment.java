import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class experiment {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("Basic").master("local[4]").getOrCreate();
        Dataset<Row> people = spark.read().json("/Users/sdasgupta1/Documents/results.txt");
        people.printSchema();
        System.out.println();
        System.out.println(createSchema());
    }
    // The inferred schema can be visualized using the printSchema() method
    public static StructType createSchema(){
        StructType schema = createStructType(new StructField[]{

                                createStructField("deviceType", StringType, true),
                                createStructField("ipAddress", StringType, true),
                                createStructField("userAgent", StringType, true),
                                createStructField("version", IntegerType, true),
                                createStructField("packetVersion", IntegerType, true),
                                createStructField("sessionID", LongType, true),
                                createStructField("sequence", IntegerType, true),
                                createStructField("buildVersion", StringType, true),
                                createStructField("deviceID", StringType, true),
                                createStructField("accountID", StringType, true),
                                createStructField("expIds", StringType, true),
                                createStructField("appName", StringType, true),
                                createStructField("connectionType", StringType, true),
                                createStructField("bootInfo", StringType, true),
                                createStructField("dataSizeGroup", StringType, true),
                                createStructField("platformVersion", StringType, true),
                                createStructField("screenInfo", StringType, true),
                                createStructField("offset", IntegerType, true),
                                createStructField("metric", IntegerType, true),
                                createStructField("previousMetric", IntegerType, true),
                                createStructField("latency", IntegerType, true),
                                createStructField("timestamp", LongType, true),
                                createStructField("x", IntegerType, true),
                                createStructField("y", IntegerType, true),
                                createStructField("itemType", IntegerType, true),
                                createStructField("itemAction", IntegerType, true),
                                createStructField("pDateTime", StringType, true),
                createStructField("sieEvent", StringType, true)

        });
        return schema;
    }

}
