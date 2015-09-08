import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");

        job.setMapperClass(LinkCountMap.class);
        job.setMapOutputKeyClass(Text.class); // id of page to which link exists
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(OrphanPageReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class); // only page id needed

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String pageId = value.toString().replaceFirst(":\\s.*", "");
            String[] linkedPageIds = value.toString().replaceFirst("[0-9]+:\\s", "").split("\\s");

            IntWritable pageIdInt = new IntWritable(Integer.parseInt(pageId));
            context.write(pageIdInt, new IntWritable(1));

            for (String id:  linkedPageIds) {
                IntWritable idInt = new IntWritable(Integer.parseInt(id));
                context.write(idInt, new IntWritable(1));
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer linksCount = 0;
            for (IntWritable value: values) {
                linksCount += value.get();
            }

            if (linksCount == 0) {
                context.write(key, NullWritable.get());
            }
        }
    }
}