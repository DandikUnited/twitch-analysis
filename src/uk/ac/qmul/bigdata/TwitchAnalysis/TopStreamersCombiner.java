package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopStreamersCombiner extends
Reducer<Text, TextIntPair, Text, TextIntPair> {
	private TextIntPair UserViewers = new TextIntPair();
	public void reduce(Text key, Iterable<TextIntPair> values, Context context)
			throws IOException, InterruptedException {
		
		int max = Integer.MIN_VALUE;
		Text topStreamer = null;
		for(TextIntPair i: values){
			if(i.getSecond().get() > max){
				max = i.getSecond().get();
				topStreamer = i.getFirst();
			}
		}
		if(topStreamer != null){
		UserViewers.set(topStreamer, new IntWritable(max));
		context.write(key, UserViewers);
		}
	}

}
