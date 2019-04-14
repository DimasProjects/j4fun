package task1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.Lag;
import org.apache.spark.sql.catalyst.expressions.Lead;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions.*;
import java.security.MessageDigest;
import java.util.LinkedList;
import java.util.List;


import static task1.CommonUtils.byteToHex;

/**
 * Created by DmitriyBrosalin on 13/04/2019.
 */
public class Task2 {

    public static void main(String[] args){

        if(args.length < 2){
            System.exit(-1);
        }

        String pathToFile = args[0];
        String pathToWrite = args[1];

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Analysis of users' activity")
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.udf().register("sessionHash", (UDF1<String, String>) element -> {
            MessageDigest finalCrypt = MessageDigest.getInstance("SHA-1");
            finalCrypt.reset();
            finalCrypt.update(element.getBytes("UTF-8"));
            return byteToHex(finalCrypt.digest());
        }, DataTypes.StringType);

        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load(pathToFile);

        dataset = dataset
                .withColumn("changed",
                        functions.when(
                        functions.lag(dataset.col("category"), 1)
                                .over(Window.partitionBy("userId").orderBy("eventTime"))
                                .equalTo(dataset.col("category")), 0).otherwise(1));
        dataset = dataset
                .withColumn("ses_num",
                        functions.sum(dataset.col("changed"))
                                .over(Window.partitionBy("userId")
                                        .orderBy("eventTime")
                                        .rowsBetween(Window.unboundedPreceding(), Window.currentRow())));
        dataset = dataset.groupBy("userId", "category", "ses_num")
                .agg(functions.min(dataset.col("eventTime")).alias("sessionStartTime"),
                        functions.max(dataset.col("eventTime")).alias("sessionEndTime"));

        dataset = dataset.select(dataset.col("userId"),
                dataset.col("category"),
                dataset.col("sessionStartTime"),
                dataset.col("sessionEndTime"),
                functions.callUDF("sessionHash",
                        functions.concat(dataset.col("userId"), dataset.col("category"))).alias("sessionId"));

        Dataset<Row> init = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load(pathToFile);

        init = init.alias("id")
                .join(dataset.alias("tt"), init.col("userId").equalTo(dataset.col("userId")), "inner")
                .where(dataset.col("category").equalTo(init.col("category")))
                .where(init.col("eventTime").between(dataset.col("sessionStartTime"), dataset.col("sessionEndTime")))
                .select("id.userId", "id.category", "id.product",
                        "id.eventType", "id.eventTime", "tt.sessionStartTime",
                        "tt.sessionEndTime", "tt.sessionId");

        init.show();

        init.write().csv(pathToWrite);

        sparkSession.close();

    }

}
