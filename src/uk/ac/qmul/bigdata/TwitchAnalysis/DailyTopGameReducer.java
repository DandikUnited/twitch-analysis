package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DailyTopGameReducer extends Reducer<Text, TextIntPair, Text, Text> {



	public void reduce(Text key, Iterable<TextIntPair> values, Context context)
			throws IOException, InterruptedException {

		HashMap<Text,IntWritable> gamesViewers = new HashMap<Text,IntWritable>();

		//total viewers
		for (TextIntPair value : values) {
			if(gamesViewers.containsKey(value.getFirst())){
				gamesViewers.put(value.getFirst(), new IntWritable(gamesViewers.get(value.getFirst()).get() + value.getSecond().get()));
			}else{
				gamesViewers.put(value.getFirst(), new IntWritable(value.getSecond().get()));
			}
		}

		Entry<Text,IntWritable> maxEntry = null;

		for(Entry<Text,IntWritable> entry : gamesViewers.entrySet()) {
			if (maxEntry == null || entry.getValue().get() > maxEntry.getValue().get()) {
				maxEntry = entry;
			}
		}
		Text bestGame = new Text(maxEntry.getKey().toString() + "\t" + maxEntry.getValue().toString());
		context.write(key, bestGame);
	}
}

