import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class TopNDriver {
    @Test
    public  void test() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(TopNDriver.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,
                new Path("D:\\InvertedIndex\\TopN\\input"));
        FileOutputFormat.setOutputPath(job,
                new Path("D:\\InvertedIndex\\TopN\\output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 :1);
    }
}
