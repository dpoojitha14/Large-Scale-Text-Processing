import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.io.WritableComparable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Stripes
{
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private MapWritable map = new MapWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			ArrayList<String> al = new ArrayList<String> ();
			StringTokenizer itr = new StringTokenizer(value.toString());	
			//Adding words
			while(itr.hasMoreTokens())
			{
				al.add(itr.nextToken());
			}
			
			for (int i = 0; i < al.size(); i++)
			{
				String term = al.get(i);
				
				map.clear();
				if(i+1 < al.size())
				{
					if (map.containsKey(al.get(i+1)) )
					{
						IntWritable count = (IntWritable) map.get(al.get(i+1));
						count.set(count.get() + 1);
						map.put(new Text(al.get(i+1)), count);
					} 
					else
					{
						map.put(new Text(al.get(i+1)), one);
					}
				}

				word.set(term);
				context.write(word, map);
			}
		}
	}
		
	
	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, Text>
	{
		private MapWritable result;

		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
		{
			result = new MapWritable();
			Iterator<MapWritable> iter = values.iterator();

			while (iter.hasNext()) 
			{
				for (Entry<Writable, Writable> e : iter.next().entrySet())
				{
					Writable inner_key = e.getKey();
					IntWritable count1 = (IntWritable) e.getValue();
					if (result.containsKey(inner_key)) {
						IntWritable count = (IntWritable) result.get(inner_key);
						count.set(count.get() + count1.get());
					} 
					else
					{
						result.put(inner_key, count1);
					}
				}
			}
			StringBuffer value = new StringBuffer();
			for (Entry<Writable, Writable> e : result.entrySet()) 
			{
				Writable key2 = e.getKey();
				Writable value2 = e.getValue();
				String final_val = key2.toString() + ", " + value2.toString() + "; ";
				value.append(final_val);
			}
			context.write(key, new Text(value.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Stripes.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}