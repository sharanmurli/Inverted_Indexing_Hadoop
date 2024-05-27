
/**Importing the necessary Libraries rerquired to perform Map Reduve in Java */
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bigram_word_index {

    public static class Bigram_Map_Job extends Mapper<Object, Text, Text, Text> {
        private Text word_index = new Text();
        private HashSet<String> targetBigrams = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // Define target bigrams
            targetBigrams.add("computer science");
            targetBigrams.add("information retrieval");
            targetBigrams.add("power politics");
            targetBigrams.add("los angeles");
            targetBigrams.add("bruce willis");
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", 2);
            if (parts.length == 2) {
                String documentId = parts[0];
                String text = parts[1].replaceAll("[^A-Za-z ]+", " ").toLowerCase();

                String[] words = text.split("\\s+");
                for (int i = 0; i < words.length - 1; i++) {
                    String bigram = words[i] + " " + words[i + 1];
                    if (targetBigrams.contains(bigram)) {
                        word_index.set(bigram);
                        context.write(word_index, new Text(documentId + ":1"));
                    }
                }
            }
        }
    }

    public static class Bigram_Reduce_Job extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> docCounts = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String docId = parts[0];
                int count = Integer.parseInt(parts[1]);
                docCounts.merge(docId, count, Integer::sum);
            }

            StringBuilder result = new StringBuilder();
            for (Map.Entry<String, Integer> entry : docCounts.entrySet()) {
                if (result.length() > 0) {
                    result.append("  ");
                }
                result.append(entry.getKey()).append(":").append(entry.getValue());
            }

            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "specific bigram index");
        job.setJarByClass(Bigram_word_index.class);
        job.setMapperClass(Bigram_Map_Job.class);
        job.setReducerClass(Bigram_Reduce_Job.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
