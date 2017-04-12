/*
 * Student Info: Name=Yujie Peng, ID=12237
 * Subject: CS570_HW3_Full_2016
 * Author: yujie
 * Filename: AverageByStudent.java
 * Date and Time: Nov 2, 2016 12:06:50 PM
 * Project Name: YujiePeng_12237_CS557_HW3
 */
package npu.yujiepeng_12237_cs557_hw3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AverageByStudent extends Configured implements Tool {
//public class AverageByStudent  {
    public static class MyMapper extends
            Mapper<LongWritable, Text, StudentCourseKey, DoubleWritable> {
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("=====in map=====");
            int sum = 0;
            double count = 0f;
            double average = 0f;
            if (key.get() >= 0) {
                String[] line = value.toString().split(",");

                String dept = line[1];
                String course = line[2];
                String name = line[3];
                
                
                for (int i = 4; i < line.length; i++){
                    sum += Integer.parseInt(line[i]);
                    count++;
                }
                average = sum/count;
                StudentCourseKey sKey = new StudentCourseKey(new Text(dept), new Text(course),new Text(name), new DoubleWritable(count) );
                System.out.println(sKey.dept +","+ sKey.course +","+ sKey.name +","+ sKey.count);
                System.out.println(average);
                context.write(sKey, new DoubleWritable(average));
            }     
        }
    }

    public static class MyReducer extends
            Reducer<StudentCourseKey, DoubleWritable, NullWritable, Text> {
        
        
        @Override
        public void reduce(StudentCourseKey key,
                Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("=====in reduce=====");
             double sum = 0f;
             double countNum = 0f;
             double countSum = 0f;
             double average = 0f;
             StringBuilder out = new StringBuilder("");
             for (DoubleWritable v : values){
//             while(values.iterator().hasNext()){
//                 DoubleWritable v = values.iterator().next();
                 
                 out.append(key.name).append(",").append(key.course).append(",").append(v.toString()).append("\n");
                 
                 countNum =  Double.parseDouble(key.count.toString());
                 countSum += countNum;
                 sum += countNum * Double.parseDouble(v.toString());
                 average = sum / countSum;    
             }
             out.append("     Overall Average: " +average +"\n" );
             context.write(NullWritable.get(), new Text(out.toString()));

        }
    }

    public static class MyPartitioner extends Partitioner<StudentCourseKey, DoubleWritable> {
        @Override
        public int getPartition(StudentCourseKey key, DoubleWritable value, int numPartitions) {
            System.out.println("=====in partition=====");
            //return Math.abs(key.dept.hashCode() % numPartitions);
            //return Math.abs(key.dept.toString().hashCode() % numPartitions);
            if (key.dept.toString().length()==3) return 0 % numPartitions;
            else return 1 % numPartitions;
            
        }
    }

    @Override
    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(AverageByStudent.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(StudentCourseKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(StudentGroupingComparator.class);
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* Delete output filepath if already exists */
        FileSystem fs = FileSystem.newInstance(getConf());
        Path outputFilePath = new Path(args[1]);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(new AverageByStudent(), args);
        System.exit(res);
    }   
}
