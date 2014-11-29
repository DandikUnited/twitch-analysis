package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopStreamersMapper extends Mapper<Object, TwitchDataRecord, Text, IntWritable> { 
		private Text user = new Text();
		private IntWritable one = new IntWritable(1);
	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	
	    	user.set(value.getUser());
	    	
	    	context.write(user, one);
	    }
}