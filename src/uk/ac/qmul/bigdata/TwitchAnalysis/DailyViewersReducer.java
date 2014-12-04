package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyViewersReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	// combiner finds peak for each channel + date
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sumViewers = 0;
		for (IntWritable value : values) {
			sumViewers += value.get();
		}
		
		context.write(key, new IntWritable(sumViewers));
	}
}