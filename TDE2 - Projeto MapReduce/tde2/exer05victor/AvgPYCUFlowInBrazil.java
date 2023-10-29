package advanced.tde2.exer05victor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AvgPYCUFlowInBrazil {

    // Job creation and IO setting
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path inputFile = new Path(files[0]); // Input file
        Path outputFile = new Path(files[1]); // Output file

        // Job creation (with its name)
        Job j = Job.getInstance(c, "AvgPYCUFlowInBrazil");


        // Setting Job classes
        j.setJarByClass(AvgPYCUFlowInBrazil.class);
        j.setMapperClass(MapForYearCat.class);
        j.setReducerClass(ReduceForCategoryYear.class);





        // Setting Job output classes
        j.setMapOutputKeyClass(YearCategoryUnitType.class);
        j.setMapOutputValueClass(FloatWritable.class);
        j.setOutputKeyClass(YearCategoryUnitType.class);
        j.setOutputKeyClass(FloatWritable.class);

        // Registering IO files
        FileInputFormat.addInputPath(j, inputFile);
        FileOutputFormat.setOutputPath(j, outputFile);

        // Launching the job and awaiting its execution
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    // Valor médio das transações por ano, categoria e tipo de unidade considerando
    //somente as transações do tipo exportação (flow) realizadas no Brasil
        public static class MapForYearCat extends Mapper<LongWritable, Text, YearCategoryUnitType, FloatWritable> {

        // Map function
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String[] columns = value.toString().split(";");

            String country = columns[0];
            String category = columns[9];
            String unitType = columns[7];
            String flow = columns[4];



            try { // Filters any non-numeric value in the year column
                Text Brasil = new Text(country);
                Text String = new Text("Brazil");
                Text flowType = new Text(flow);
                Text export = new Text("Export");

                if(Brasil.equals(String) && flowType.equals(export) ){
                    int year = Integer.parseInt(columns[1]);
                    long price = Integer.parseInt(columns[5]);
                    con.write(new YearCategoryUnitType(year,category,unitType),new FloatWritable(price) );
                }
            } catch (NumberFormatException e) {
                System.out.println("Non-numeric year found.");
            }
        }

    }

    public static class ReduceForCategoryYear extends Reducer<YearCategoryUnitType, FloatWritable, YearCategoryUnitType, FloatWritable> {

        // Reduce function
        public void reduce(YearCategoryUnitType key, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException {
            // Adding all values from one key
            float totalAmount = 0;
            int n = 0;
            for (FloatWritable value : values) {
                totalAmount = value.get();
                n++;
            }
            float average = totalAmount/n;
            // Returning the pair key-value as (year, totalAmount)
            con.write(key, new FloatWritable(average));
        }

    }


}
