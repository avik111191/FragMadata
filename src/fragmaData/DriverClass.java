package fragmaData;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DriverClass {
	
	 public static class CountMapper extends Mapper<Object, Text, Text, IntWritable>
	    {
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	        {
	          
	        		String[] record=value.toString().split("::");
	                context.write(
	                					new Text(record[1]),
	                					new IntWritable(
	                									Integer.parseInt(record[0].toString())
	                									)
	                			);
	            
	        }
	    }
	 public static class CountCombiner
	    extends Reducer<Text,IntWritable,Text,IntWritable>
	    {
	        public void reduce(Text key, Iterable<IntWritable> values,
	        Context context) throws IOException, InterruptedException
	        {
	    
	            Iterator<IntWritable> valuesIt = values.iterator();
	            while(valuesIt.hasNext()){
	            	IntWritable p=valuesIt.next();
		            context.write(key, new IntWritable(1));

	            	
	            	        }

	        }
	    }
	 
	 public static class CountResuder
	    extends Reducer<Text,IntWritable,Text, Text>
	    {
	        public void reduce(Text key, Iterable<IntWritable> values,
	        Context context) throws IOException, InterruptedException
	        {
	        	IntWritable count=new IntWritable();
	        	int i=0;
	            Iterator<IntWritable> valuesIt = values.iterator();
	            while(valuesIt.hasNext()){
	            	            i = i + valuesIt.next().get();
	            	        }
	            	        

	            count.set(i);
	            context.write(null,new Text(key.toString()+','+count.toString()));
	        }
	    }
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "countViews");
        job.setJarByClass(DriverClass.class);
        
        job.setMapperClass(CountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setCombinerClass(CountCombiner.class);

        job.setReducerClass(CountResuder.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


	}

}
