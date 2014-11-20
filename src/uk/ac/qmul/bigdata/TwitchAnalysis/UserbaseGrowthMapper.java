package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserbaseGrowthMapper extends Mapper<Object, TwitchDataRecord, Text, IntWritable> { 

	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	Text a = new Text();
	    	String b = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(value.getTimeStamp().get()));
	    	a.set(b);
	    	context.write(a, new IntWritable(1));
	    }
}