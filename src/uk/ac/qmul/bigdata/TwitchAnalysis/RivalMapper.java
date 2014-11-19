package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RivalMapper extends Mapper<Object, TwitchDataRecord, Text, IntWritable> { 
	    private final IntWritable one = new IntWritable(1);
	    private Text data = new Text();
	    String dump;
	    int startIndex;
	    
	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	context.write(value.getGame(), new IntWritable(1));
	    }
}