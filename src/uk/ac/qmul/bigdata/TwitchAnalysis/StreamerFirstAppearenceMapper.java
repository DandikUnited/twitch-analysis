package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StreamerFirstAppearenceMapper extends Mapper<Object, TwitchDataRecord, Text, LongWritable> { 
	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	context.write(value.getUser(), value.getTimeStamp());
	    }
}