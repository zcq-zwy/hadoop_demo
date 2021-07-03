import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.TreeMap;

public class TopNMapper extends
        Mapper<LongWritable, Text, NullWritable, IntWritable> {
    private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();
    @Override
    public void map(LongWritable key, Text value, Context context){
        String line = value.toString();
        String[] nums = line.split(" ");
        for (String num : nums) {
            repToRecordMap.put(Integer.parseInt(num), " ");
            if (repToRecordMap.size() > 5) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
    }
    @Override
    protected void cleanup(Context context){
        for (Integer i : repToRecordMap.keySet()){
            try {
                context.write(NullWritable.get(), new IntWritable(i));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
