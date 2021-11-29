import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public class DataExtract {

  public static class DataMap extends Mapper <Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      context.write(new Text(tokens[1]), new Text(tokens[2] + "," + tokens[3] + "," + tokens[4] + "," + tokens[5] + "," + tokens[6] + "," + tokens[7] + "," + tokens[9]
                                                   + "," + tokens[10] + "," + tokens[11] + "," + tokens[12] + "," + tokens[13] + "," + tokens[14]
                                                   + "," + tokens[18] + "," + tokens[20] + "," + tokens[21]));
    }
  }

  public static class DataReduce extends Reducer <Text, Text, Text, NullWritable> {

    private TreeMap<String, String> data;
    private TreeMap<String, String> title;

    @Override
    protected void setup(Context context) {
      data = new TreeMap<String, String>();
      title = new TreeMap<String, String>();
    }

    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text temp : values) {
        if (key.toString().equals("ID")) {
          title.put(key.toString(), temp.toString());
        }
        if (!temp.toString().contains("NaN") && !key.toString().equals("ID")) {
          data.put(key.toString(), temp.toString());
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<String, String> entry : title.entrySet()) {
        String temp = entry.getKey() + "," + entry.getValue();
        context.write(new Text(temp), NullWritable.get());
      }
      for (Map.Entry<String, String> entry : data.entrySet()) {
        String temp = entry.getKey() + "," + entry.getValue();
        context.write(new Text(temp), NullWritable.get());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Data Extract");
    job.setJarByClass(DataExtract.class);
    job.setMapperClass(DataExtract.DataMap.class);
    job.setReducerClass(DataExtract.DataReduce.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
