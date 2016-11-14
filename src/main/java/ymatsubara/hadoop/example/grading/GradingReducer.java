package ymatsubara.hadoop.example.grading;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer<C, D, E, F>
// C: Reducer Input Key, D: Reducer Input Key (same to C and D in GradingMapper)
// E: Reducer Output Key, F: Reducer Output Value
public class GradingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static int reduceNumber = 0;
    @Override
    protected void setup(Context context) {
        this.reduceNumber++;
        System.out.println("Reduce" + String.valueOf(this.reduceNumber) + ": Start");
    }

    // reduce(C, D, Context)
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String studentId = key.toString();
        int totalScore = 0;
        for (IntWritable value : values) {
            int score = value.get();
            totalScore += score;
        }
        context.write(new Text(studentId), new IntWritable(totalScore));
        // or you can use the following instead
        //context.write(key, new IntWritable(totalScore));
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Reduce" + String.valueOf(this.reduceNumber) + ": End");
    }
}
