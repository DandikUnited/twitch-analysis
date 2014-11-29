package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RivalMapperBridge extends Mapper<Text, IntWritable, Text, IntWritable>{
private IntWritable result = new IntWritable(0);
	

	public void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		
		
		context.write(key, value);

	}

}
