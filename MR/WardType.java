import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
public class WardType {
    public static class TokenizerMapper extends Mapper < Object, Text, Text, IntWritable > {
        private Text node;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            IntWritable one = new IntWritable(1);
            context.write(value, one);
        }
    }

    public static class CountReducer extends Reducer < Text, IntWritable, Text, Text > {
        private Set < String > wardType;
        @Override
        protected void setup(Context context) {
            wardType = new HashSet < String > ();
        }
        @Override
        protected void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException,
        InterruptedException{
            int sum = 0;
            for (IntWritable value : values) {
                sum++;
            }
            String su = ","+String.valueOf(sum)+",";
            context.write(key, new Text(su));
            sum = 0;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct nodes new");
        job.setJarByClass(WardType.class);
        job.setMapperClass(WardType.TokenizerMapper.class);
        job.setReducerClass(WardType.CountReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
