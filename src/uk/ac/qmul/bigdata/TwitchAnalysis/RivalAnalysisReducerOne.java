package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RivalAnalysisReducerOne extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result;

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int peak = 0;

		for (IntWritable value : values) {

			if (peak < value.get()) {
				peak = value.get();
			}

		}
		String[] keyParts = key.toString().split("\t");
		key = new Text(keyParts[1] + "\t" + keyParts[2]);

		result = new IntWritable(peak);
		context.write(key, result);

	}
}
