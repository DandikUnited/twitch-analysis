package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StreamersGrowthMapper extends Mapper<Object, Text, Text, IntWritable> { 
		private Text day = new Text();
		private IntWritable one = new IntWritable(1);
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String date = value.toString().split("\t")[1];
	    	day.set(date);
	    	context.write(day, one);
	    	
	    }
}