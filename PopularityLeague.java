import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {
    Path tmpPath = new Path("/mp2/tmp");

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(tmpPath, true);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[0]);

        createPropagationJob(conf, inputPath);
        return createAggregationJob(conf, outputPath);
    }

    private void createPropagationJob(Configuration conf, Path inputPath) throws Exception {
        Job job = Job.getInstance(conf, "Propagation");
        //jobA.setOutputKeyClass(IntWritable.class);
        //jobA.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PropagationMapper.class);
        job.setReducerClass(PropagationReducer.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.setJarByClass(PopularityLeague.class);
        job.waitForCompletion(true);
    }

    private Integer createAggregationJob(Configuration conf, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Aggregation");

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);

        //job.setMapOutputKeyClass(NullWritable.class);
        //job.setMapOutputValueClass(IntArrayWritable.class);

        job.setMapperClass(AggregationMapper.class);
        job.setReducerClass(AggregationReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, tmpPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(PopularityLeague.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static class PropagationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class PropagationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        }
    }

    public static class AggregationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class AggregationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        }
    }
}