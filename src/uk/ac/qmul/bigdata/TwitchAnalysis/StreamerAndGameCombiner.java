package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StreamerAndGameCombiner extends Reducer<Text, IntIntPair, Text, IntIntPair> {

	@Override
	public void reduce(Text compositeKey, Iterable<IntIntPair> values,
			Context context) throws IOException, InterruptedException {

		int totalViewers = 0;
		int totalStreamRecords = 0;
		for (IntIntPair i : values) {
			totalViewers += i.getFirst().get();
			totalStreamRecords += i.getSecond().get();
		}
		
		IntIntPair totalAverageData = new IntIntPair(totalViewers, totalStreamRecords);

		context.write(compositeKey, totalAverageData);
	}

}
