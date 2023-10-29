package advanced.tde2.exer02azevedo;

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

public class TransactionsPerYear {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "transactionsPerYear");

        // Setting Job classes
        j.setJarByClass(TransactionsPerYear.class);
        j.setMapperClass(MapForTransactionsPerYear.class);
        j.setReducerClass(ReduceForTransactionsPerYear.class);

        // Setting Job output classes
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputKeyClass(IntWritable.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForTransactionsPerYear extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        // Map function
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split(";");

            try { // Filters any non-numeric value in the year column
                IntWritable year = new IntWritable(Integer.parseInt(columns[1]));

                // Returning the pair key-value as (year, 1)
                con.write(year, new IntWritable(1));
            } catch (NumberFormatException e) {
                System.out.println("Non-numeric year found.");
            }
        }

    }

    public static class ReduceForTransactionsPerYear extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        // Reduce function
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // Adding all values from one key
            int totalAmount = 0;
            for (IntWritable value : values) {
                totalAmount += value.get();
            }

            // Returning the pair key-value as (year, totalAmount)
            con.write(key, new IntWritable(totalAmount));
        }

    }

}
