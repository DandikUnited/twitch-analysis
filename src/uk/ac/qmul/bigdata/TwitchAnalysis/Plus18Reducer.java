package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Plus18Reducer extends
		Reducer<Text, IntWritable, IntWritable, Text> {
	
	private IntWritable result = new IntWritable(0);
	

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();

		}
		result.set(sum);
		context.write(result,key);

	}

}
