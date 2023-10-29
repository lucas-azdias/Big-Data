package advanced.tde2.exer07azevedo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class HigherCommodityAmountPerFlow2016 {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path midFile = new Path(files[1]); // Intermediary file
        Path outputFile = new Path(files[2]); // Output file

        // Jobs creation (with its names)
        Job j1 = Job.getInstance(c, "commodityAmount2016");
        Job j2 = Job.getInstance(c, "higherCommodityAmountPerFlow2016");

        // Setting Jobs classes
        j1.setJarByClass(HigherCommodityAmountPerFlow2016.class);
        j1.setMapperClass(MapForCommodityAmount2016.class);
        j1.setReducerClass(ReduceForCommodityAmount2016.class);
        j2.setJarByClass(HigherCommodityAmountPerFlow2016.class);
        j2.setMapperClass(MapForHigherCommodityAmountPerFlow2016.class);
        j2.setReducerClass(ReduceForHigherCommodityAmountPerFlow2016.class);

        // Setting Job output classes
        j1.setMapOutputKeyClass(CommodityFlow.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(CommodityFlow.class);
        j1.setOutputKeyClass(LongWritable.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CommodityAmount.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputKeyClass(CommodityAmount.class);

        // Registering IO files
        FileInputFormat.addInputPath(j1, inputFile);
        FileOutputFormat.setOutputPath(j1, midFile);
        FileInputFormat.addInputPath(j2, midFile);
        FileOutputFormat.setOutputPath(j2, outputFile);

        // Launching the jobs and awaiting theirs executions
        System.exit(j1.waitForCompletion(true) && j2.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForCommodityAmount2016 extends Mapper<LongWritable, Text, CommodityFlow, LongWritable> {

        // Map function
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split(";");

            String commodity = columns[3];
            String flow = columns[4];

            try { // Filters any non-numeric value in the amount and year column
                int year = Integer.parseInt(columns[1]);
                LongWritable amount = new LongWritable(Long.parseLong(columns[8]));

                if (year == 2016) {
                    // Returning the pair key-value as (CommodityFlow(commodity, flow), amount)
                    con.write(new CommodityFlow(commodity, flow), amount);
                }
            } catch (NumberFormatException e) {
                System.out.println("Non-numeric amount found.");
            }
        }

    }

    public static class ReduceForCommodityAmount2016 extends Reducer<CommodityFlow, LongWritable, CommodityFlow, LongWritable> {

        // Reduce function
        public void reduce(CommodityFlow key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {
            // Adding all values from one key
            long totalAmount = 0;
            for (LongWritable value : values) {
                totalAmount += value.get();
            }

            // Returning the pair key-value as (CommodityFlow(commodity, flow), totalAmount)
            con.write(key, new LongWritable(totalAmount));
        }

    }

    public static class MapForHigherCommodityAmountPerFlow2016 extends Mapper<LongWritable, Text, Text, CommodityAmount> {

        // Map function
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");

            String commodity = columns[0];
            String flow = columns[1];
            long amount = Long.parseLong(columns[2]);

            // Returning the pair key-value as (flow, CommodityAmount(commodity, amount))
            con.write(new Text(flow), new CommodityAmount(commodity, amount));
        }

    }

    public static class ReduceForHigherCommodityAmountPerFlow2016 extends Reducer<Text, CommodityAmount, Text, CommodityAmount> {

        // Reduce function
        public void reduce(Text key, Iterable<CommodityAmount> values, Context con) throws IOException, InterruptedException {
            // Getting CommodityAmount with the highest amount
            CommodityAmount higher = new CommodityAmount(null, Long.MIN_VALUE);
            for (CommodityAmount value : values) {
                if (Long.compare(value.getAmount(), higher.getAmount()) > 0) {
                    higher.setCommodity(value.getCommodity());
                    higher.setAmount(value.getAmount());
                }
            }

            // Returning the pair key-value as (flow, CommodityAmount(commodity, amount))
            con.write(key, higher);
        }

    }

}
