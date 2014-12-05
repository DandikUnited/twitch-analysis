package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StreamerFirstAppearenceReducer extends
Reducer<Text, LongWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		LongWritable lowest = new LongWritable(Long.MAX_VALUE);

		for(LongWritable i: values){
			if(i.get() < lowest.get()){
				lowest = i;
			}
		}
		Text textDate = new Text();
		textDate.set(formatter.format(new Date(lowest.get())));
		context.write(key, textDate);
	}

}
