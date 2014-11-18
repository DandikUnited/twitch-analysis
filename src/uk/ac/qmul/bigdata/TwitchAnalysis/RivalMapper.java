package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RivalMapper extends Mapper<Object, Text, Text, IntWritable> { 
	    private final IntWritable one = new IntWritable(1);
	    private Text data = new Text();
	    String dump;
	    int startIndex;
	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	Date dateObj = new Date();
	    	dump = value.toString();
	        if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
	            startIndex = StringUtils.ordinalIndexOf(dump,"	",2) + 1;
	            
	            		//data.set(hashtagarray[i]);
	            		//bla bla
	            		context.write(data, one);
	            		}
	            	}
	            }