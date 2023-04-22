package de.microtema;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable intWritable = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        intWritable.set(sum);

        context.write(key, intWritable);
    }
}