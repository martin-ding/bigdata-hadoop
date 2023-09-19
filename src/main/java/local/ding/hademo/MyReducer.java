package local.ding.hademo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable valueout = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable longWritable: values) {
            sum += longWritable.get();
        }
        valueout.set(sum);
        context.write(key, valueout);
    }
}
