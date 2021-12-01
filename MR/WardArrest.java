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
public class WardArrest {
    public static class TokenizerMapper extends Mapper < Object, Text, Text, Text > {
        private Text node;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
	    String[] valuesArr = value.toString().split(",");
	    Text type = new Text(valuesArr[0]);
	    Text arrest = new Text(valuesArr[1]);
            context.write(type, arrest);
        }
    }

    public static class CountReducer extends Reducer < Text, Text, Text, Text > {
        private Set < String > typeArrest;
        @Override
        protected void setup(Context context) {
            typeArrest = new HashSet < String > ();
        }
        @Override
        protected void reduce(Text key, Iterable < Text > values, Context context) throws IOException,
        InterruptedException{
            double arrestTrue = 0;
            double arrestFalse = 0;
            double sum = 0;
            for (Text value : values) {
                sum++;
                if(value.toString().equals("True")){
                    arrestTrue++;
                }
                if(value.toString().equals("False")){
                    arrestFalse++;
                }

            }

            String res = ","+String.valueOf(arrestTrue) +","+ String.valueOf(arrestFalse)+","+
                String.valueOf(Math.round(((arrestTrue/sum)*100.0)*100.0)/100.0)+","+String.valueOf(Math.round(((arrestFalse/sum)*100.0)*100.0)/100.0)+",";
         	 context.write(key, new Text(res));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct nodes new");
        job.setJarByClass(WardArrest.class);
        job.setMapperClass(WardArrest.TokenizerMapper.class);
        job.setReducerClass(WardArrest.CountReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
