package de.microtema;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable INT_WRITABLE = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        var stringTokenizer = new StringTokenizer(value.toString());

        while (stringTokenizer.hasMoreTokens()) {
            word.set(stringTokenizer.nextToken());
            context.write(word, INT_WRITABLE);
        }
    }
}