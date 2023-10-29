package advanced.tde2.exer01gabriel;
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

public class TransactionsInBrazil {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "transactionsInBrazil");

        // Setting Job classes
        j.setJarByClass(TransactionsInBrazil.class);
        j.setMapperClass(MapForTransactionsInBrazil.class);
        j.setReducerClass(ReduceForTransactionsInBrazil.class);

        // Setting Job output classes
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForTransactionsInBrazil extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Map function
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split(";");

            try { // Filters any non-numeric value in the year column
                Text country = new Text(columns[0]);
                Text brasil = new Text("Brazil");

                // Returning the pair key-value if country equals a "Brazil"
                if(country.equals(brasil))
                    con.write(country, new IntWritable(1));
            } catch (NumberFormatException e) {
                System.out.println("Non-numeric year found.");
            }
        }

    }

    public static class ReduceForTransactionsInBrazil extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Reduce function
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // Adding all values from one key
            int totalAmount = 0;
            for (IntWritable value : values) {
                totalAmount += value.get();
            }

            // Returning the pair key-value as (key, totalAmount)
            con.write(key, new IntWritable(totalAmount));
        }

    }

}
