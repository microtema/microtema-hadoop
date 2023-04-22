package de.microtema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {

        var path = WordCount.class.getResource("/").getPath();

        var inputPath = path + "input.txt";
        var outPath = path + "../output/" + System.currentTimeMillis();

        System.out.printf("Hadoop Word Counter from [%s] -> [%s]%n", inputPath, outPath);

        var configuration = new Configuration();

        var job = new Job(configuration, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}