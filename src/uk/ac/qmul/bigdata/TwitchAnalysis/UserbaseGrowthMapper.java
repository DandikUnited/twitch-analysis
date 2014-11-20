package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserbaseGrowthMapper extends Mapper<Object, TwitchDataRecord, LongWritable, IntWritable> { 

	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	context.write(value.getTimeStamp(), new IntWritable(1));
	    }
}