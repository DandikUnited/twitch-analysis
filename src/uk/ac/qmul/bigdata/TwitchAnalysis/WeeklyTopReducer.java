package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeeklyTopReducer extends
Reducer<Text, TextIntPair, Text, Text> {

	public void reduce(Text key, Iterable<TextIntPair> values, Context context)
			throws IOException, InterruptedException {

		
		//player, <Sum, count>
		HashMap<Text, IntIntPair> playerViewersSum = new HashMap<Text, IntIntPair>();

		//player, viewers
		for(TextIntPair i: values){
			if(playerViewersSum.containsKey(i.getFirst())){
				//updating value
				playerViewersSum.put(i.getFirst(), new IntIntPair(playerViewersSum.get(i.getFirst()).getFirst().get() + i.getSecond().get(), playerViewersSum.get(i.getFirst()).getSecond().get() + 1));
			}else{
				playerViewersSum.put(i.getFirst(), new IntIntPair(i.getSecond().get(), 1));
			}
		}


		// Snipped adapted from http://stackoverflow.com/questions/7498751/get-the-keys-with-the-biggest-values-from-a-hashmap
		Entry<Text,IntIntPair> maxEntry = null;

		for(Entry<Text,IntIntPair> entry : playerViewersSum.entrySet()) {
			if (maxEntry == null || (entry.getValue().getFirst().get()/entry.getValue().getSecond().get()) >  (maxEntry.getValue().getFirst().get()/maxEntry.getValue().getSecond().get())) {
				maxEntry = entry;
			}
		}
		Text best = new Text(maxEntry.getKey().toString() + "\t" + (maxEntry.getValue().getFirst().get()/ maxEntry.getValue().getSecond().get()));
		context.write(key, best);

	}

}
