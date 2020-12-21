package project_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class M_Matrix implements Writable {
	public int i;
	public double Mv;

	M_Matrix() {}

	M_Matrix(int i, double Mv) {
		this.i = i;
		this.Mv = Mv;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeDouble(Mv);
	}

	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		Mv = in.readDouble();
	}

}

class N_Matrix implements Writable {

	public int j;
	public double Nv;

	N_Matrix() {}

	N_Matrix( int j, double Nv) {
		this.j = j;
		this.Nv = Nv;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(j);
		out.writeDouble(Nv);
	}

	public void readFields(DataInput in) throws IOException {
		j = in.readInt();
		Nv = in.readDouble();
	}
}

class M_N_Matrix implements Writable {

	public short tag;
	public M_Matrix m_matrix;
	public N_Matrix n_matrix;

	M_N_Matrix() {}

	M_N_Matrix(M_Matrix m) {
		tag = 0;
		m_matrix = m;
	}

	M_N_Matrix(N_Matrix n) {
		tag = 1;
		n_matrix = n;
	}

	public void write(DataOutput out) throws IOException {
		out.writeShort(tag);
		if (tag == 0) {
			m_matrix.write(out);
		} else {
			n_matrix.write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		tag = in.readShort();
		if (tag == 0) {
			m_matrix = new M_Matrix();
			m_matrix.readFields(in);
		} else {
			n_matrix = new N_Matrix();
			n_matrix.readFields(in);
		}
	}

}

class Pair implements WritableComparable<Pair> {
	int i;
	int j;

	Pair() {}

	Pair(int i, int j) {
		this.i = i;
		this.j = j;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		i = in.readInt();
		j = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(i);
		out.writeInt(j);

	}

	
	@Override
	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		
		if(this.i < o.i)
			return -1;
		else if(this.i == o.i)
			if(this.j == o.j)
				return 0;
			else if (this.j < o.j)
				return -1;
			else
				return 1;
		else
			return 1;
	}
	
	
	 public String toString() {return this.i+"\t"+this.j;}
	 

}

public class Multiply {

	public static class M_MatricesMapper extends Mapper<Object, Text, IntWritable, M_N_Matrix> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i= s.nextInt();
			int j = s.nextInt();
			double v = s.nextDouble();
			M_Matrix m = new M_Matrix(i,v);
			context.write(new IntWritable(j), new M_N_Matrix(m));
			s.close();
		}
	}

	public static class N_MatricesMapper extends Mapper<Object, Text, IntWritable, M_N_Matrix> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i= s.nextInt();
			int j = s.nextInt();
			double v = s.nextDouble();
			N_Matrix n = new N_Matrix(j,v);
			context.write(new IntWritable(i), new M_N_Matrix(n));
			s.close();
		}
	}

	public static class ResultReducer extends Reducer<IntWritable, M_N_Matrix, Pair, DoubleWritable> {
		static Vector<M_Matrix> m_matrix = new Vector<M_Matrix>();
		static Vector<N_Matrix> n_matrix = new Vector<N_Matrix>();

		@Override
		public void reduce(IntWritable key, Iterable<M_N_Matrix> values, Context context)
				throws IOException, InterruptedException {
			m_matrix.clear();
			n_matrix.clear();
			for (M_N_Matrix v : values)
				if (v.tag == 0)
					m_matrix.add(v.m_matrix);

				else
					n_matrix.add(v.n_matrix);
			for (M_Matrix m : m_matrix) {
				for (N_Matrix n : n_matrix) {
					context.write(new Pair(m.i, n.j), new DoubleWritable(m.Mv * n.Nv));
				}
			}
		}
	}

	public static class SecondMapper extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {
		@Override
		public void map(Pair p, DoubleWritable value, Context context) throws IOException, InterruptedException {
			context.write(p,value );
		}
	}

	public static class SecondReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {

		public void reduce(Pair p, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double result = 0;
			for (DoubleWritable v : values) {
				result  += v.get();
			}
			context.write(p, new DoubleWritable(result));
		}
	}

	public static void main(String[] args) throws Exception {
		Path outputPath=new Path(args[2]);
		Configuration conf1 = new Configuration();
		Job job = Job.getInstance(conf1);
		job.setJobName("MultiplyJob");
		job.setJarByClass(Multiply.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(M_N_Matrix.class);
		
		job.setReducerClass(ResultReducer.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, M_MatricesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, N_MatricesMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		int code = job.waitForCompletion(true)? 0:1;
		
		if(code ==0) {
		 // Configuration conf2 = new Configuration(); 
		  Job job2 = Job.getInstance(conf1);
		  job2.setJarByClass(Multiply.class);
		  job2.setJobName("ResultJob");
		 
		  job2.setMapperClass(SecondMapper.class);
		  //job.setNumReduceTasks(0);
		  job2.setReducerClass(SecondReducer.class);
		  
		  job2.setOutputKeyClass(Pair.class);
		  job2.setOutputValueClass(DoubleWritable.class);
		  
		  job2.setMapOutputKeyClass(Pair.class);
		  job2.setMapOutputValueClass(DoubleWritable.class);
		  
		  job2.setInputFormatClass(SequenceFileInputFormat.class);
		  job2.setOutputFormatClass(TextOutputFormat.class);
		  
		  
		  
		  TextInputFormat.setInputPaths(job2, outputPath);
		  TextOutputFormat.setOutputPath(job2,new Path(args[3]));
		 // outputPath.getFileSystem(conf1).deleteOnExit(outputPath);
		  System.exit(job2.waitForCompletion(true)?0:1);	 
		}
	}
}
