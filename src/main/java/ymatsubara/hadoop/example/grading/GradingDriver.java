package ymatsubara.hadoop.example.grading;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GradingDriver {
    public static void main(String[] args) throws Exception {
        Path inputDirPath = new Path("src/main/resources/input/grading/");
        Path outputDirPath = new Path("src/main/resources/output/grading/");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:/");
        conf.set("mapreduce.framework.name", "local");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(outputDirPath, true);
        fs.setWriteChecksum(false);
        Job job = Job.getInstance(conf, "Quiz grading");
        job.setMapperClass(GradingMapper.class);
        job.setReducerClass(GradingReducer.class);
        job.setNumReduceTasks(10);
        // Output Key and Value: same to E and F in GradingReducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputDirPath);
        FileOutputFormat.setOutputPath(job, outputDirPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}