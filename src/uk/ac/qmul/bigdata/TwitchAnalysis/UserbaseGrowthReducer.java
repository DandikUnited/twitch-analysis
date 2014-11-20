package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserbaseGrowthReducer extends
		Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for(IntWritable i: values)
			sum += i.get();
		
		context.write(key, new IntWritable(sum));
	}

}
