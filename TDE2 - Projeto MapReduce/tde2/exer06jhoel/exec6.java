package advanced.tde2.exer06jhoel;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


import org.apache.hadoop.io.IntWritable;




public class exec6 {
    // Método principal da classe
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada - alterar nas configurações
        Path input = new Path(files[0]);

        // arquivo de saida - alterar nas configurações
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "MaiorMenorTrasactionCriminosoExec6");

        // Registro das classes
        j.setJarByClass(exec6.class); // main
        j.setMapperClass(exec6.MapTransaction6.class); // mapper
        j.setReducerClass(exec6.TransactionReducer6.class); // reducer

        // Definição dos tipos de saída (map e reduce)



        j.setMapOutputKeyClass(KeyYearCategory.class); // chave saída do map
        j.setMapOutputValueClass(Composto.class); // valor saída do map
        j.setOutputKeyClass(KeyYearCategory.class); // chave saída do reduce
        j.setOutputValueClass(Text.class); // valor saída do reduce


        // Cadastrar arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); // entrada
        FileOutputFormat.setOutputPath(j, output); // saída

        // Execução do job
        j.setNumReduceTasks(1); // Define o número de tarefas de redução como 1 para obter um único arquivo de saída

        System.exit(j.waitForCompletion(true)  ? 0 : 1);

    }


    public static class MapTransaction6 extends Mapper<LongWritable, Text, KeyYearCategory, Composto> {


        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            try { //Pelo visto e necessario por causa da primeira linha do csv, na hora de converter para float, se nao voce fica 4 horas a toa lendo seu cod sem entender o erro
                if (fields.length == 10) {
                    String year = fields[1];
                    String Flow =fields[4];
                    String commodity = fields[3];
                    String category = fields[9];
                    long commodityID = Long.parseLong(fields[2]);
                    double price = Float.parseFloat(fields[5]);





                    KeyYearCategory outKey = new KeyYearCategory(year, category);


                    context.write(outKey, new Composto(commodity, Flow,  category,  commodityID ,  price));

                }
            } catch (NumberFormatException e) {
                System.out.println("Dado nao numerico.");
            }

        }

    }


    public static class TransactionReducer6 extends Reducer<KeyYearCategory, Composto, KeyYearCategory, Text> {
        private Text result = new Text();

        String espacos = new String(new char[10]).replace('\0', ' '); // para deixar o formatod e saida mais clean
        public void reduce(KeyYearCategory key, Iterable<Composto> values, Context context) throws IOException, InterruptedException {
            long maxCommodityID = Long.MIN_VALUE;
            long minCommodityID = Long.MIN_VALUE;
            String flowMax;
            String flowMin;
            String commodityMax;
            String commodityMin;
            double maxPrice = Double.MIN_VALUE;
            double minPrice = Double.MAX_VALUE;
            Composto maxTransaction = null;
            Composto minTransaction = null;

            for (Composto composto : values) {
                double price = composto.getPrice();

                if (price > maxPrice) {
                    maxTransaction = composto;
                }

                if (price < minPrice) {
                    minTransaction = composto;
                }
            }
            maxPrice = maxTransaction.getPrice();
            minPrice = minTransaction.getPrice();

            flowMax = maxTransaction.getFlow();
            flowMin =maxTransaction.getFlow();

            commodityMax = maxTransaction.getCommodity();
            commodityMin = minTransaction.getCommodity();

            maxCommodityID = maxTransaction.getCommodityID();
            minCommodityID = maxTransaction.getCommodityID();




            maxPrice = maxTransaction.getPrice();
            minPrice = minTransaction.getPrice();

            flowMax = maxTransaction.getFlow();
            flowMin = minTransaction.getFlow();

            commodityMax = maxTransaction.getCommodity();
            commodityMin = minTransaction.getCommodity();

            maxCommodityID = maxTransaction.getCommodityID();
            minCommodityID = minTransaction.getCommodityID();

            String valorSaida = "MAIORTransacao: Valor= " + maxPrice + " Fluxo= " + flowMax + " Mercadoria= " + commodityMax + " ID= " + maxCommodityID + "    \t     MENORTransacao:   Valor= " + minPrice + " Fluxo= " + flowMin + " Mercadoria= " + commodityMin + " ID= " + minCommodityID;

            result.set(valorSaida);
            context.write(key, result);
           // context.write(key, maxTransaction);
           // context.write(key, minTransaction);
        }
    }
}
