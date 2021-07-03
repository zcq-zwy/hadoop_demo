import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

public class DedupDriver {
    @Test
    public  void test()
            throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job =Job.getInstance(conf);

        job.setJarByClass(DedupDriver.class);
        job.setMapperClass(DedupMapper.class);
        job.setReducerClass(DedupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,
                new Path("D:\\InvertedIndex\\Dedup\\input"));
        FileOutputFormat.setOutputPath(job,
                new Path("D:\\InvertedIndex\\Dedup\\output"));
        job.waitForCompletion(true);
    }

}
