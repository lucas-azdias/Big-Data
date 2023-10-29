package advanced.tde2.exer03jhoel;



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

/* Classe principal (WordLen) que retorna a quantidade palavras de um dado comprimento. Por exemplo: 4, 223 significa que temos 223 palavras de comprimento igual a 4
Dentro dessa classe, teremos:
i) Método principal (main) que realiza as configurações do Hadoop (onde está o arquivo de entrada, qual será o arquivo de saída, registro das classes, etc)
ii) Uma classe MapX que estende a classe Mapper do Hadoop
    ii-b) Um método map que irá percorrer cada linha do arquivo de entrada e gerar (chave, valor)
iii) Uma classe ReduceX que estende a classe Reducer do Hadoop
    iii-b) Um método reduce que irá receber a saída do map e gerar (chave, valor)
 */
public class Exer3 {
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
        Job j = new Job(c, "Exer3");

        // Registro das classes
        j.setJarByClass(Exer3.class); // main
        j.setMapperClass(Exer3.MapExer.class); // mapper
        j.setReducerClass(Exer3.TransactionReducer.class); // reducer

        // Definição dos tipos de saída (map e reduce)



        j.setMapOutputKeyClass(FlowYear.class); // chave saída do map
        j.setMapOutputValueClass(IntWritable.class); // valor saída do map
        j.setOutputKeyClass(FlowYear.class); // chave saída do reduce
        j.setOutputValueClass(IntWritable.class); // valor saída do reduce


        // Cadastrar arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); // entrada
        FileOutputFormat.setOutputPath(j, output); // saída

        // Execução do job
        j.setNumReduceTasks(1); // Define o número de tarefas de redução como 1 para obter um único arquivo de saída

        j.waitForCompletion(true);
    }

    /*
    "Parâmetros" da classe Mapper:
    Tipo 1: tipo da chave de entrada
    Tipo 2: tipo do valor de entrada
    Tipo 3: tipo da chave de saída
    Tipo 4: tipo do valor de saída

    ARQUIVO TEXTO COMO ENTRADA
    - Input: (offset/qtde bytes lidos no arquivo, conteúdo da linha)
     */
    public static class MapExer extends Mapper<LongWritable, Text, FlowYear, IntWritable> {

        private Text flowYear = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            if (fields.length == 10) {
                String year = fields[1];
                String flow = fields[4];

                // Verificar se é uma exportação ou importação
                if (flow.equals("Export") || flow.equals("Import")) {
                   // flowYear.set(flow + "\t" + year);
                    FlowYear flowYear = new FlowYear(flow, year);
                    // Emitir a chave (tipo de fluxo e ano) e o valor (1)
                    context.write(flowYear, one);
                }
            }
        }

    }


    public static class TransactionReducer extends Reducer<FlowYear, IntWritable, FlowYear, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(FlowYear key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            System.out.println("Reduce");
            // Contar o número de transações para a chave dada
            for (IntWritable value : values) {
                count += value.get();
            }

            result.set(count);

            // Emitir a chave (tipo de fluxo e ano) e o valor (número de transações)
            context.write(key, result);
        }
    }
}
