package dg.hadoop.platform.homework.ch06;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryGlobalSort {

    public static class EmpMapper extends
            Mapper<Object, Text, FloatWritable, Text> {
        private Text name = new Text();
        private FloatWritable sal = new FloatWritable();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split("\\s+");
            String empname = "";
            float salary = 0;
            float comm = 0;
            if (parts != null) {
                empname = parts[1];
                if (parts.length == 6) {
                    try {
                        salary = Float.parseFloat(parts[4]);
                    } catch (Exception e) {
                        salary = 0;
                    }
                } else  if (parts.length == 7){
                    try {
                        salary = Float.parseFloat(parts[5]);
                    } catch (Exception e) {
                        salary = 0;
                    }
                }else  if (parts.length == 8){
                    try {
                        salary = Float.parseFloat(parts[5]);
                        comm = Float.parseFloat(parts[6]);
                    } catch (Exception e) {
                        salary = 0;
                        comm = 0;
                    }
                }
                name.set(empname);
                sal.set(salary + comm);
                context.write(sal, name);
            }
        }
    }

    public static class EmpReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {

        private Text result = new Text();

        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                result.set(val.toString());
                context.write(result, key);
            }
        }
    }

    public static class FloatComparator extends WritableComparator{
        protected FloatComparator(){
            super(FloatWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SalariesGlobalSort");
        job.setJarByClass(SalaryGlobalSort.class);

        // TODO: specify a mapper
        job.setMapperClass(EmpMapper.class);
        // TODO:specify a reducer
        job.setReducerClass(EmpReducer.class);
        job.setSortComparatorClass(FloatComparator.class);

        // TODO: specify output types
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true))
            return;
    }
}

