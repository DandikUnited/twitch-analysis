package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StreamerAndGameMapper extends Mapper<Object, TwitchDataRecord, Text, IntIntPair> { 
    
    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
    	Text compositeKey = new Text(value.getGame() + "\t" + value.getUser());
    	
    	if(value.getGame().getLength() > 0){ // filtering empty games
    		context.write(compositeKey, new IntIntPair(value.getViewers().get(), 1));
    	}
    }
}