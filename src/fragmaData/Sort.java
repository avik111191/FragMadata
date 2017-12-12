package fragmaData;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;


public class Sort {
	
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
	    extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>
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
	            	context.write(valuesIt.next(),key);
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
        Job job = new Job(conf, "sort");
		job.setNumReduceTasks(1);
        job.setJarByClass(Sort.class);
        
        job.setMapperClass(sortmap.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setReducerClass(sortRedeuce.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}
