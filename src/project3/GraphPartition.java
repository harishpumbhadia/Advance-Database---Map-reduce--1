package project3;

import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
    public long id;                   // the vertex ID
    public  Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    Vertex() {}
    
    Vertex(long id,long centroid,short depth,Vector<Long> adjacent){
    	this.id = id;
    	this.centroid = centroid;
    	this.depth = depth;
    	this.adjacent = adjacent;
    }
    
    
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readLong();
		centroid = in.readLong();
		depth = in.readShort();
		int length = in.readInt();
    	Vector<Long> adj =new Vector<Long>(length);
    	for(int i = 0 ; i<length;i++) {
    		adj.add(in.readLong());
    	}
		adjacent = adj;
	}
	@Override
	public  void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);
		out.writeInt(adjacent.size());
		for(int i=0; i< adjacent.size(); i++) {
			out.writeLong(adjacent.get(i));
		}
	}
	
	public String toString() {
		String a = adjacent.toString();
		a = a.replace("[", "");
		a= a.replace("]", "");
		a = a.replace(",", "");
		return this.id+" "+this.centroid+" "+this.depth+" "+this.adjacent;
	}
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_	 = 8;
    static short BFS_depth = 0;
    public static class FirstJobMapper extends Mapper<Object, Text, LongWritable, Vertex> {
		
		private int count=0;
	
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long id = s.nextLong();
			long centroidId;
			Vector<Long> adjacent = new Vector<Long>();
			
			while(s.hasNextLong()) { 
				adjacent.add(s.nextLong());
			}
			 
			if(count< 10)
				 centroidId = id;
			else
				 centroidId = -1;
							
			context.write(new LongWritable(id), new Vertex(id,centroidId,BFS_depth,adjacent));
			count++;			
			s.close();
		}
	}
    
    public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(value.id),value);
			if(value.centroid >0) {
				for(Long n : value.adjacent) {
					context.write(new LongWritable(n), new Vertex(n,value.centroid,BFS_depth,new Vector<Long>()));
				}
			}
		}
	}
    
    public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
    
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {
			short min_depth =1000;
			
	    	Vertex m = new Vertex(key.get(),(long)-1,BFS_depth,new Vector<Long>());
	    	for(Vertex v : values) {
	    		if(v.adjacent.isEmpty() == false) 
	    			m.adjacent = v.adjacent;
	    		if(v.centroid >0 && v.depth< min_depth) {
	    			min_depth =v.depth;
	    			m.centroid = v.centroid;
	    		}
	    	}
	    	m.depth = min_depth;
			context.write(key, m);
		}
	}
    
    public static class ThirdMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
		@Override
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(value.centroid),new LongWritable(1));
		}
	}
    
    public static class ThirdReducer extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {
    
		public void reduce(LongWritable LongWritable, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long m = 0;
			for(LongWritable v: values) {
				m = m + v.get();
			}
			context.write(LongWritable, new LongWritable(m));
		}
	}

    public static void main ( String[] args ) throws Exception {
    	
    	//Configuration conf1 = new Configuration();
        Job job = Job.getInstance();
        job.setJobName("MyFirstJob");
        job.setJarByClass(GraphPartition.class);
        
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
		
        job.setMapperClass(FirstJobMapper.class);
        
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/i0"));
        /* ... First Map-Reduce job to read the graph */
		int code = job.waitForCompletion(true)? 0:1;
		
		if(code == 0) {
        
        for (short i = 0; i < 6; i++ ) {
            BFS_depth++;
            Job job2 = Job.getInstance();
            job2.setJobName("MySecondJob");
            job2.setJarByClass(GraphPartition.class);
            
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            job2.setMapOutputKeyClass(LongWritable.class);
    		job2.setMapOutputValueClass(Vertex.class);
    		
    		job2.setMapperClass(SecondMapper.class);
    		job2.setReducerClass(SecondReducer.class);
    		
    		job2.setOutputKeyClass(LongWritable.class);
    		job2.setOutputValueClass(Vertex.class);
    		
    		FileInputFormat.setInputPaths(job2,new Path(args[1]+"/i"+i));
    		FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/i"+(i+1)));
          //  job2 = Job.getInstance(conf2);
            /* ... Second Map-Reduce job to do BFS */
            job2.waitForCompletion(true);
        }
		
        /* ... Final Map-Reduce job to calculate the cluster sizes */
		//System.exit(job2.waitForCompletion(true)?0:1);
        
        Job job3 = Job.getInstance();
        job3.setJobName("MyFirstJob");
        job3.setJarByClass(GraphPartition.class);
        
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
		
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
		
        job3.setMapperClass(ThirdMapper.class);
        job3.setReducerClass(ThirdReducer.class);
        
		FileInputFormat.setInputPaths(job3,new Path(args[1]+"/i8"));
		
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		System.exit(job3.waitForCompletion(true)?0:1);
		}
    }
}
