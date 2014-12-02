package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, Text, Text> { 
    	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String[] data = value.toString().split("\t");
    	String game = data[0].replace(" ", "").toLowerCase();
    	
    	context.write(new Text(game), value);
    }
}