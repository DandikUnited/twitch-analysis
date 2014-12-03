package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GamePopularityReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	// combiner finds peak for each channel + date
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int peak = 0;
		for (IntWritable value : values) {
			if (peak < value.get())
				peak = value.get();

		}
		context.write(key, new IntWritable(peak));

	}
}
