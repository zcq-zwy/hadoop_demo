import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class InvertedIndexDriver {
    @Test
    public void test()
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InvertedIndexDriver.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,
                new Path("D:\\InvertedIndex\\input"));
        //指定处理完成之后的结果所保存的位置     output文件夹不需创建 自动生成
        FileOutputFormat.setOutputPath(job,
                new Path("D:\\InvertedIndex\\output"));

        //向YARN集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
