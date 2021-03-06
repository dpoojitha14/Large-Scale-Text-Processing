import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

public class Activity1
{
	public static HashMap<String,ArrayList> lemmas = new HashMap<String,ArrayList>();
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text location = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{		 
			String str = value.toString();
			
			String result="",result2="";
            if(str.length() > 0)
			{
            	result = str.substring(str.indexOf("<") + 1, str.indexOf(">"));
            	result2 = str.substring(str.indexOf(">")+1, str.length()-1);
            	result2.trim();
			}
			
			ArrayList<String> al = new ArrayList<String> ();
			StringTokenizer itr = new StringTokenizer(result2);	
			//Adding words
			while(itr.hasMoreTokens())
			{
				String term = itr.nextToken();
				//System.out.println("before "+term);
				term = term.replaceAll("j","i");
				term = term.replaceAll("v", "u");
				//System.out.println("after "+term);
				al.add(term);
			}
			
			for (int i = 0; i < al.size(); i++)
			{
				String term1 = al.get(i);
				
				String[] tokens = result.split("\\.");
				 String docid = tokens[0].trim();
				 String ch = tokens[tokens.length-2].trim();
				 String line = tokens[tokens.length-1].trim();
				 
				 String loc = docid + "[" + ch + "," + line + "]";
				 location.set(loc);
				
				if(lemmas.containsKey(term1))
				{
					ArrayList list = lemmas.get(term1);
					
					for(int j=0;j<list.size();j++)
					{
						context.write(new Text((String) list.get(j)), location);
					}
				}
				
				else
				{
					context.write(new Text(term1), location);
				}

			}
		}
	}
		
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable< Text >values, Context context) throws IOException, InterruptedException
		{
			
			Iterator<Text> iter = values.iterator();
			
			String val1="[";
			
			while(iter.hasNext())
			{
				val1 = val1+ (iter.next()).toString()+",";
			}
			val1 = val1 + "]";
			context.write(key, new Text(val1));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Activity1.class);
	 
		BufferedReader br = null;
		FileReader fr = null;
	
		try {

			fr = new FileReader("/home/hadoop/new_lemmatizer.csv");
			br = new BufferedReader(fr);
			BufferedWriter bw = null;
			FileWriter fw = null;

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) 
			{
				//System.out.println(sCurrentLine);
				String[] tokens = sCurrentLine.split(",");
				ArrayList l = new ArrayList();
				for(int i=1;i<tokens.length;i++)
				{
					String item = tokens[i];
					l.add(item);
				}
				lemmas.put(tokens[0], l);
			}
		}catch(IOException e)
		{
			e.printStackTrace();
		}

		
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}