package fragmaData;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import fragmaData.Sort.sortRedeuce;
import fragmaData.Sort.sortmap;

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

	 

	 public static class sortmap extends Mapper<Object, Text, LongWritable, LongWritable>
	    {
		    private Logger logger = Logger.getLogger(sortmap.class);

	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	        {
	          
	        		String[] record=value.toString().split(",");
	        		logger.info("read "+record[0]+record[1]);
	        		if(record.length==2)
	    	                context.write(new LongWritable(Integer.parseInt(record[1].toString())),new LongWritable(Integer.parseInt(record[0].toString())));
	            
	        }
	    }
	
	 public static  class sortRedeuce
	    extends Reducer<LongWritable,LongWritable,Text,Text>
	    {
		  int c=0;
		    private Logger logger = Logger.getLogger(sortRedeuce.class);

	        public void reduce(LongWritable key, Iterable<LongWritable> values,
	        Context context) throws IOException, InterruptedException
	        {
	        	try {
	        	if(values!=null){
	        	Iterator<LongWritable> valuesIt = values.iterator();
	        	logger.info(key.toString()+"--"+values.toString());
	        	if(c < 10)
	            while(valuesIt.hasNext()){
	            	context.write(null,new Text(valuesIt.next().toString()+","+key.toString()));
	            	c++;
	            			if(c > 10) {
	            						break;
	            					   }
	            	        }


	        }
	        	}
	        	catch(Exception exp) {
	        		logger.error(exp.getMessage());
	        	}
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
        job.waitForCompletion(true); 
        //Thread.wait();
     /*   int p=0;
        while(p<99999999) {
        	p++;
        }
        */
        
        Job job1 = new Job(conf, "sort");
		job1.setNumReduceTasks(1);
        job1.setJarByClass(Sort.class);
        
        job1.setMapperClass(sortmap.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job1.setReducerClass(sortRedeuce.class);

        FileInputFormat.addInputPath(job1, new Path(args[2]+"/"+"part-r-00000"));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);


        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);


	}

}
