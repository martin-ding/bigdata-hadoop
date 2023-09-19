package local.ding.hademo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text text = new Text();
    LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] s = str.split(" ");
        for (String strr : s) {
            text.set(strr);
           context.write(text, one);
        }
    }
}
