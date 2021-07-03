import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DedupMapper extends Mapper<LongWritable,
        Text, Text, NullWritable> {
    //<0, 2021-6-1 a><11, 2021-6-2 b>
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        //NullWritable.get()方法设置空值
        context.write(value, NullWritable.get());
    }

}
