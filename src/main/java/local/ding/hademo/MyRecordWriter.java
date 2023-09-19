package local.ding.hademo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text, LongWritable> {
    FSDataOutputStream fs2;
    FSDataOutputStream fs1;

    public MyRecordWriter(TaskAttemptContext job) {
        try (FileSystem fileSystem = FileSystem.get(job.getConfiguration())) {
            fs1 = fileSystem.create(new Path("/Users/dingmac/Downloads/hademo/output/output1/1.txt"));
            fs2 = fileSystem.create(new Path("/Users/dingmac/Downloads/hademo/output/output1/2.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, LongWritable longWritable) throws IOException, InterruptedException {
        String string = text.toString();
        if (string.contains("liu")) {
            fs1.writeBytes(string + "\t" + longWritable.toString() + "\n");
        } else {
            fs2.writeBytes(string + "\t" + longWritable.toString() + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fs1);
        IOUtils.closeStream(fs2);
    }
}
