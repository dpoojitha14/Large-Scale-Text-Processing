import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.WritableComparable;

class TextPair extends MapWritable {
	
	Text firstword;
	Text secondword;
	
	TextPair()
	{
		firstword = new Text();
		secondword =  new Text();
	}
	
	public void set(String first, String second) 
	{
		this.firstword.set(first);
		this.secondword.set(second);
	}
	
	public Text getFirst()
	{
		return firstword;
	}
	
	public Text getSecond() {
		return secondword;
	}
	
	public void setFirst(String first)
	{
		this.firstword.set(first);
	}
	
	public void setSecond(String second)
	{
		this.secondword.set(second);
	}
	
	public int compareTo(TextPair tePair)
	{
		int compare = firstword.compareTo(tePair.getFirst());
		if (0 == compare) {
			compare = secondword.compareTo(tePair.getSecond());
		}
		return compare;
	}
	
	public boolean equals(Object obj) 
	{
		boolean isEqual =  false;
		if (obj instanceof TextPair) {
			TextPair iPair = (TextPair)obj;
			isEqual = firstword.equals(iPair.firstword) && secondword.equals(iPair.secondword);
		}
		
		return isEqual;
	}
	
	public String toString()
	{
		return (firstword.toString()) + "," + (secondword.toString());
	}

	
}


public class Pairs {

	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		TextPair pair = new TextPair();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
	
			ArrayList<String> al = new ArrayList<String> ();
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			//Adding words
			while(itr.hasMoreTokens())
			{
				al.add(itr.nextToken());
			}
			
			TextPair pair = new TextPair();
			
			int length = al.size();
			
			for(int i=0;i<length;i++)
			{
				String first = al.get(i);
				if(i<length-1)
				{
					String second = al.get(i+1);
					pair.set(first, second);
				}
			}
			
			Text words = new Text();
			words.set(pair.toString());
			
			context.write(words, one);
		
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
            context.write(key, result);
        }
		
	}
	
	public static void main(String[] args) throws Exception
	{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Pairs.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}
