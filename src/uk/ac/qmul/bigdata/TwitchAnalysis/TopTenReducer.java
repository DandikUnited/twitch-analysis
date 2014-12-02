package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenReducer extends Reducer<Text, Text, Text, Text> {

	public final static int RECORD_LIMIT = 10;
	public final static double POPULARITY_THRESHOLD = 7000.0;


	@Override
	public void reduce(Text gameName, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		TreeMap<Double, String[]> testMap= new TreeMap<Double, String[]>();
		
		for (Text t : values) {
			String[] recordData = t.toString().split("\t");
			Double averageNumber = Double.parseDouble(recordData[2]);

			testMap.put(averageNumber, recordData);
			if (testMap.size() > RECORD_LIMIT) {
				testMap.remove(testMap.firstKey());
			}
		}
		
		if(testMap.firstKey() < POPULARITY_THRESHOLD){ // skipping game if the least popular channel is less than threshold
			return;
		}
		
		for(String[] recordData : testMap.values()){
			context.write(new Text(recordData[0]), new Text(recordData[1] + "\t" + recordData[2]));
		}
	}
}