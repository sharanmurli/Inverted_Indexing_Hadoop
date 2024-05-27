/**Importing the necessary Libraries rerquired to perform Map Reduve in Java */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unigram_word_index {

  public static class Unigram_Map_Job extends Mapper<Object, Text, Text, Text> {
    private Text word_index = new Text();
    private Text docID_Count = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\t", 2);
      if (parts.length == 2) {
        String documentId = parts[0];
        String text = parts[1].replaceAll("[^A-Za-z]+", " ").toLowerCase();
        StringTokenizer itr = new StringTokenizer(text);
        HashMap<String, Integer> wordCount = new HashMap<>();

        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          wordCount.put(token, wordCount.getOrDefault(token, 0) + 1);
        }

        for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
          word_index.set(entry.getKey());
          docID_Count.set(documentId + ":" + entry.getValue());
          context.write(word_index, docID_Count);
        }
      }
    }
  }

  public static class Unigram_Reduce_Job extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      for (Text val : values) {
        if (sb.length() > 0)
          sb.append("  ");
        sb.append(val.toString());
      }
      context.write(key, new Text(sb.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Unigram Index - Word Count");
    job.setJarByClass(Unigram_word_index.class);
    job.setMapperClass(Unigram_Map_Job.class);
    job.setReducerClass(Unigram_Reduce_Job.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
