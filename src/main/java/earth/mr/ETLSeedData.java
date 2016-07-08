package earth.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created on 2016/7/6
 *
 * @author feng.wei
 */
public class ETLSeedData {

    private static Logger logger = LoggerFactory.getLogger(ETLSeedData.class);

    /**
     * mapper
     */
    public static class SeedDataMapper extends TableMapper<Text, Text> {

        private static Logger loggerMapper = LoggerFactory.getLogger(SeedDataMapper.class);
        private static HTable detailTable = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            detailTable = new HTable(configuration, TableName.valueOf("cstor:test01"));
        }

        /**
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            loggerMapper.info("ImmutableBytesWritable-key = " + Bytes.toString(key.get()));
            loggerMapper.info("row-key = " + Bytes.toString(value.getRow()));

            Cell cell = value.getColumnLatestCell(Bytes.toBytes("f"), Bytes.toBytes("99"));
            if (null != cell) {
                byte[] valueBytes = CellUtil.cloneValue(cell);
                loggerMapper.info("cloValue = " + Bytes.toString(valueBytes));
                if (null != valueBytes) {
                    context.write(new Text(Bytes.toString(value.getRow())), new Text(valueBytes));
                }

            }

            context.write(new Text(Bytes.toString(value.getRow())), new Text(Bytes.toString(value.getRow())));

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (null != detailTable) {
                detailTable.close();
            }
        }
    }


    public static class SeedDataReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text value : values) {
                stringBuilder.append(value.toString());
            }
            context.write(key, new Text(stringBuilder.toString()));

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            logger.error("parameters occur to error");
            System.exit(1);
        }

        // 指定输出分隔符
        conf.set("mapreduce.output.textoutputformat.separator", ":");
        Job job = Job.getInstance(conf);
        String condition = remainingArgs[0];
        if (condition != null && !"".equals(condition)) {
            job.setJobName("ETLSeedData-MR-" + condition);
        }

        job.addCacheArchive(new URI(""));

        Scan scan = new Scan();
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("9"));
        scan.setFilter(filter);

        TableMapReduceUtil.initTableMapperJob("cstor:test01"
                , scan
                , SeedDataMapper.class
                , Text.class
                , Text.class
                , job);

        job.setReducerClass(SeedDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("/user/cstor/mr/output5"));
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        System.exit(0);

    }
}
