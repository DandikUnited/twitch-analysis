package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StreamerFirstAppearenceCombiner extends
Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		LongWritable lowest = new LongWritable(Long.MAX_VALUE);

		for(LongWritable i: values){
			if(i.get() < lowest.get()){
				lowest = i;
			}
		}
		context.write(key, lowest);
	}

}
