package ymatsubara.hadoop.example.grading;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper<A, B, C, D>
// A: Mapper Input Key, B: Mapper Input Value
// C: Mapper Output Key, D: Mapper Output Key
public class GradingMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private static int mapNumber = 0;
    @Override
    protected void setup(Context context) {
        this.mapNumber++;
        System.out.println("Map" + String.valueOf(this.mapNumber) + ": Start");
    }

    // map(A, B, Context), A: LongWritable and B: Text if sample.input = file(s)
    @Override
    public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();
        String[] keyValue = line.split("\t");
        String studentId = keyValue[0];
        int score = Integer.parseInt(keyValue[1]);
        // context.write(C, D);
        context.write(new Text(studentId), new IntWritable(score));
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Map" + String.valueOf(this.mapNumber) + ": End");
    }
}
