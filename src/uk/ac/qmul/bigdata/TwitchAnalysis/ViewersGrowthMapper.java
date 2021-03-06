package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ViewersGrowthMapper extends Mapper<Object, TwitchDataRecord, Text, IntWritable> { 
		private Text textDate = new Text();
	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	
	    	String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.getTimeStamp().get()));
	    	textDate.set(date);
	    	
	    	context.write(textDate, value.getViewers());
	    }
}