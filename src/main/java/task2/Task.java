package task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.security.MessageDigest;

import static task1.CommonUtils.byteToHex;

/**
 * Created by DmitriyBrosalin on 13/04/2019.
 */
public class Task {

    public static void main(String[] args){
        if(args.length < 2){
            System.exit(-1);
        }

        String pathToFile = args[0];
        String pathToResultDirectory = args[1];

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Analysis of users' activity")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load(pathToFile);

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

        Dataset<Row> toAnalysis = sparkSession.sql(query);
        toAnalysis.createOrReplaceTempView("Analysis");

        //For each category find median session duration
        Dataset<Row> median = sparkSession.sql("select category, percentile_approx(delta, 0.5) as median from (\n" +
                "  select category, (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) as delta from (\n" +
                "    select category,\n" +
                "      min(sessionStartTime) over(partition by category, sessionId order by sessionStartTime) as minInGroup,\n" +
                "      max(sessionEndTime) over(partition by category, sessionId order by sessionEndTime) as maxInGroup\n" +
                "      from Analysis\n" +
                "  )\n" +
                ") group by category");
        median.write().csv(pathToResultDirectory + "/median.csv");

//        For each category find # of unique users spending less than 1 min, 1 to 5 mins and more
//        than 5 mins
        Dataset<Row> classification = sparkSession.sql("select category, description, count(userId)\n" +
                "from (\n" +
                "  select category, userId,\n" +
                "  case when (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) < 60 then 'less then 1 min'\n" +
                "       when (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) between 60 and 300 then '1 to 5 mins'\n" +
                "       when (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) > 300 then 'more than 5 mins'\n" +
                "  END\n" +
                "  as description\n" +
                "  from(\n" +
                "    select category, userId,\n" +
                "      min(sessionStartTime) over(partition by category, userId order by sessionStartTime) as minInGroup,\n" +
                "      max(sessionEndTime) over(partition by category, userId order by sessionEndTime) as maxInGroup\n" +
                "      from Analysis\n" +
                "  )\n" +
                ") group by category, description");
        classification.write().csv(pathToResultDirectory + "/classification.csv");

        //For each category find top 10 products ranked by time spent by users on product pages
        Dataset<Row> rank = sparkSession.sql("select category,\n" +
                "    product,\n" +
                "    row_number() over(partition by category order by sumTime desc) as rank\n" +
                "    from (\n" +
                "      select category, product, sum(unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) as sumTime\n" +
                "      from(\n" +
                "        select category, product,\n" +
                "          min(sessionStartTime) over(partition by category, sessionId order by sessionStartTime) as minInGroup,\n" +
                "          max(sessionEndTime) over(partition by category, sessionId order by sessionEndTime) as maxInGroup\n" +
                "          from Analysis\n" +
                "      ) group by category, product\n" +
                "    )");
        rank.write().csv(pathToResultDirectory + "/rank.csv");
    }

}
