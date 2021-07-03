import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DedupReducer extends Reducer
        <Text, NullWritable, Text, NullWritable> {
    //<2021-6-1 a, null><2021-6-2 b, null><2021-6-3 c, null>
    @Override
    protected void reduce(Text key, Iterable<NullWritable>values, Context context)
            throws IOException, InterruptedException{
        context.write(key, NullWritable.get());
    }
}
