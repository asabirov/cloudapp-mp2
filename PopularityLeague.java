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
        Path outputPath = new Path(args[1]);

        createLinksCounterJob(conf, inputPath);
        return createPageRankerJob(conf, outputPath);
    }

    private void createLinksCounterJob(Configuration conf, Path inputPath) throws Exception {
        Job job = Job.getInstance(conf, "Links Count");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(LinksCounterMapper.class);
        job.setReducerClass(LinksCounterReducer.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.setJarByClass(PopularityLeague.class);
        job.waitForCompletion(true);
    }

    private Integer createPageRankerJob(Configuration conf, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "PagerRanker");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PageRankerMapper.class);
        job.setReducerClass(PageRankerReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, tmpPath);
        FileOutputFormat.setOutputPath(job, outputPath);


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

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LinksCounterMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        List<String> league;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String league_file = conf.get("league");
            this.league = Arrays.asList(readHDFSFile(league_file, conf).split("\n"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] linkedPageIds = value.toString().replaceFirst("[0-9]+:\\s", "").split("\\s");

            for (String id:  linkedPageIds) {
                if (league.contains(id)) {
                    System.out.println("[LinksCounterMapper] Sent to reducer: " + id);
                    IntWritable idInt = new IntWritable(Integer.parseInt(id.trim()));
                    context.write(idInt, new IntWritable(1));
                }
            }
        }
    }

    public static class LinksCounterReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static class PageRankerMapper extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer[] values = {toInteger(key), toInteger(value)};
            IntArrayWritable val = new IntArrayWritable(values);
            context.write(NullWritable.get(), val);
        }

        private Integer toInteger(Text value) {
            return Integer.parseInt((value.toString()));
        }
    }

    public static class PageRankerReducer extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> items, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable item : items) {
                Integer[] currentPage = (Integer[]) item.toArray();

                Integer pr = calculatePageRank(currentPage, items);
                System.out.println(currentPage[0] + " - " + pr);

                context.write(new IntWritable(currentPage[0]), new IntWritable(pr));
            }
        }

        private Integer calculatePageRank(Integer[] currentPage, Iterable<IntArrayWritable> pages) {
            Integer pr = 0;
            for (IntArrayWritable page : pages) {
                Integer[] pageData = (Integer[]) page.toArray();
                if (!currentPage[0].equals(pageData[0]) && currentPage[1] > pageData[1]) {
                    pr += 1;
                }
            }

            return pr;
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}