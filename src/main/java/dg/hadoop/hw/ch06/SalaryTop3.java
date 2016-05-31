package dg.hadoop.hw.ch06;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryTop3 {

    public static class EmpMapper extends Mapper<Object, Text, Text, Text> {
        private Text _key = new Text("key");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split("\\s+");
            String empname="", salary="";
            if (parts != null) {
                empname = parts[1];
                if (parts.length == 6) {
                    salary = parts[4];
                } else {
                    salary = parts[5];
                }
                context.write(_key, new Text(empname + ":" + salary));
            }
        }
    }

    public static class EmpReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String name=null, name1=null, name2=null, name3=null;
            float salary=0, salary1=0, salary2=0, salary3=0;
            int count = 0;
            String tmpName = null;
            float tmpSalary = 0;

            // process values

            for (Text val : values) {
                try {
                    String parts[] = val.toString().split(":");
                    name = parts[0];
                    salary = Float.parseFloat(parts[1]);

                    switch(count){
                        case 0:
                            name1 = name;
                            salary1 = salary;
                            count++;
                            break;
                        case 1:
                            if (salary>salary1){
                                tmpSalary = salary1;
                                tmpName = name1;
                                salary1=salary;
                                name1=name;
                                salary2=tmpSalary;
                                name2=tmpName;
                            }else{
                                salary2=salary;
                                name2=name;
                            }
                            count++;
                            break;
                        case 2:
                            if (salary>salary2){
                                tmpSalary = salary2;
                                tmpName = name2;
                                salary2=salary;
                                name2=name;
                                salary3=tmpSalary;
                                name3=tmpName;
                            }else{
                                salary3=salary;
                                name3=name;
                            }
                            if (salary2>salary1){
                                tmpSalary = salary1;
                                tmpName = name1;
                                salary1=salary2;
                                name1=name2;
                                salary2=tmpSalary;
                                name2=tmpName;
                            }
                            count ++;
                            break;
                        case 3:
                            if (salary>salary3){
                                salary3=salary;
                                name3=name;
                            }

                            if (salary>salary2){
                                tmpSalary = salary2;
                                tmpName = name2;
                                salary2=salary;
                                name2=name;
                                salary3=tmpSalary;
                                name3=tmpName;
                            }

                            if (salary2>salary1){
                                tmpSalary = salary1;
                                tmpName = name1;
                                salary1=salary2;
                                name1=name2;
                                salary2=tmpSalary;
                                name2=tmpName;
                            }

                            break;
                        default:
                            break;
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            context.write(new Text(name1), new Text(""+salary1));
            context.write(new Text(name2), new Text(""+salary2));
            context.write(new Text(name3), new Text(""+salary3));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SalariesTop3");
        job.setJarByClass(SalaryTop3.class);

        // TODO: specify a mapper
        job.setMapperClass(EmpMapper.class);
        // TODO:specify a reducer
        job.setReducerClass(EmpReducer.class);
        //job.setCombinerClass(EmpReducer.class);

        // TODO: specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true))
            return;
    }
}
