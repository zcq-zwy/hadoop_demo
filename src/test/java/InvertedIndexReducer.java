import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private static Text result = new Text();
    //输入:<MapReduce file3.txt:2>
    //输出:<MapReduce file1.txt:1; file2.txt:1; file3:2;>
    @Override
    protected void reduce(Text key, Iterable<Text>values, Context context)
            throws InterruptedException, IOException {
        //生产文档列表
        String fileList = new String();
        for (Text value : values) {
            fileList += value.toString() + ";";
        }
        result.set(fileList);
        context.write(key, result);
    }
}
