package advanced.tde2.exer04victor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AveragePriceOfTransactionsPerYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "AveragePriceOfTransactionsPerYear");

        // Setting Job classes
        j.setJarByClass(AveragePriceOfTransactionsPerYear.class);
        j.setMapperClass(AveragePriceOfTransactionsPerYear.MapForAveragePriceOfTransactionsPerYear.class);
        j.setReducerClass(AveragePriceOfTransactionsPerYear.ReduceForAveragePriceOfTransactionsPerYear.class);

        // Setting Job output classes
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(AvgPrice.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputKeyClass(FloatWritable.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAveragePriceOfTransactionsPerYear extends Mapper<LongWritable, Text, IntWritable, AvgPrice> {

        // Map function
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split(";");

            try { // Filters any non-numeric value in the year column
                int year = Integer.parseInt(columns[1]);
                long price = Integer.parseInt(columns[5]);
                // Returning the pair key-value as (year, AVGPrice(price,1))
                con.write(new IntWritable(year), new AvgPrice(price,1));

            } catch (NumberFormatException e) {
                System.out.println("Non-numeric year found.");
            }
        }

    }

    public static class ReduceForAveragePriceOfTransactionsPerYear extends Reducer<IntWritable, AvgPrice, IntWritable, FloatWritable> {

        // Reduce function
        public void reduce(IntWritable key, Iterable<AvgPrice> values, Context con) throws IOException, InterruptedException {
            // Adding all values from one key
            int totalAmount = 0;
            int totalPrice = 0;
            for (AvgPrice value : values) {
                totalAmount += value.getOcurrence();
                totalPrice += value.getPrice();
            }
            float media =  totalPrice/(float)totalAmount;


            // Returning the pair key-value as (year, medium)
            con.write(key, new FloatWritable(media));
        }

    }
}
