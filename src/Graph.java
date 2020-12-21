import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {
	//class for first job mapper
	public static class FirstJobMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
	//First job map function will assign each value 1. For example: 10,1
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            int y = s.nextInt();
            context.write(new IntWritable(x),new IntWritable(1));
            s.close();
        }
    }
	//First job reducer class
	public static class FirstJobReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
	//in first job reduce function will count the number whose values are equal . Like if 10 is repeating 3 times so it will result into 10,3
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int sum = 0;
          //  long count = 0;
            for (IntWritable v: values) {
                sum += v.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
	//This class is for second job mapper
	public static class SecondJobMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
	//This second job map method will take result of the first job which will be i.e, [10,3] [11,3]... etc
	//This map method will take total number of times that any value is repeating and assign it integer 1 to it.
	//like if it got result from 1st job which is [10,3],[8,3]...etc, ... so it will convert [3,1],[3,1]...etc
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int x = s.nextInt();
            int y = s.nextInt();
            context.write(new IntWritable(y),new IntWritable(1));
            s.close();
        }
    }
	//This class is for second job reducer
	public static class SecondJobReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
	// This method will take result from mapper class like[3,1][3,1].... and convert result into 1,2.....etc which is our final output
	// count the total number of values
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int sum = 0;
          //  long count = 0;
            for (IntWritable v: values) {
                sum += v.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    // Main method		
    @SuppressWarnings("deprecation")
	public static void main ( String[] args ) throws Exception {
    	 Configuration conf1 = new Configuration(); //first job configuration
    	 Job job = Job.getInstance(conf1); // First job instance

         job.setJobName("My First Job");
         job.setJarByClass(Graph.class);
         
         job.setOutputKeyClass(IntWritable.class);
         job.setOutputValueClass(IntWritable.class);
         
         job.setMapOutputKeyClass(IntWritable.class);
         job.setMapOutputValueClass(IntWritable.class);
         
         job.setMapperClass(FirstJobMapper.class);
         job.setReducerClass(FirstJobReducer.class);
         
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);
         
         Path outputPath=new Path("SecondMapper");
         FileInputFormat.setInputPaths(job,new Path(args[0]));
         FileOutputFormat.setOutputPath(job,outputPath);
         outputPath.getFileSystem(conf1).delete(outputPath);
         
         job.waitForCompletion(true);
         
         Configuration conf2  = new Configuration(); // Second job configuration
         Job job2 = Job.getInstance(conf2); // Second job instance
	
         job2.setJobName("My Second Job");
         job2.setJarByClass(Graph.class);
         
         job2.setOutputKeyClass(IntWritable.class);
         job2.setOutputValueClass(IntWritable.class);
         
         job2.setMapOutputKeyClass(IntWritable.class);
         job2.setMapOutputValueClass(IntWritable.class);
         
         job2.setMapperClass(SecondJobMapper.class);
         job2.setReducerClass(SecondJobReducer.class);
         
         job2.setInputFormatClass(TextInputFormat.class);
         job2.setOutputFormatClass(TextOutputFormat.class);
         
         FileInputFormat.setInputPaths(job2,outputPath);
         FileOutputFormat.setOutputPath(job2,new Path(args[1]));
         outputPath.getFileSystem(conf1).deleteOnExit(outputPath);
         System.exit(job2.waitForCompletion(true)?0:1);
   }
}
