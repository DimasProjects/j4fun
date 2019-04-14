package task1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import static task1.CommonUtils.byteToHex;

/**
 * Created by DmitriyBrosalin on 13/04/2019.
 */
public class Task1 {

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

        dataset.createOrReplaceTempView("InitData");

        String query = "with tempTable as (\n" +
                "  select userId, category, ses_start as sessionStartTime, ses_max as sessionEndTime\n" +
                "    from(\n" +
                "      select userId, category, ses_num, min(eventTime) as ses_start, max(eventTime) as ses_max\n" +
                "        from(\n" +
                "        select userId, category, eventTime,\n" +
                "          sum(changed) over(partition by userId order by eventTime rows between unbounded preceding and current row) as ses_num\n" +
                "          from (\n" +
                "          select userId, category, eventTime,\n" +
                "            case when lag(category) over(partition by userId order by eventTime) = category then 0 else 1 end as changed\n" +
                "            from InitData\n" +
                "        )\n" +
                "      )\n" +
                "      group by userId, category, ses_num\n" +
                "  )\n" +
                ")\n" +
                "\n" +
                "select id.userId, id.category,\n" +
                "       id.product, id.eventType,\n" +
                "       id.eventTime, tt.sessionStartTime,\n" +
                "       tt.sessionEndTime, sessionHash(concat(id.userId, '_', id.category)) as sessionId\n" +
                "from InitData id\n" +
                "  inner join tempTable tt\n" +
                "  on tt.userId = id.userId\n" +
                "  where id.category = tt.category\n" +
                "  and id.eventTime between tt.sessionStartTime and sessionEndTime";

        Dataset result = sparkSession.sql(query);

        result.show();

        result.write().csv(pathToWrite);

        sparkSession.close();
    }

}