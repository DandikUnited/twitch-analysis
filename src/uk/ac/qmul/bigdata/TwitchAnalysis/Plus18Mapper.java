package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import java.text.Normalizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Plus18Mapper extends
Mapper<Object, TwitchDataRecord, Text, IntWritable> {
	private final IntWritable ones = new IntWritable(1);
	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String gameName = value.getStatus().toString().toLowerCase();
		gameName = Normalizer.normalize(gameName, Normalizer.Form.NFD);
		gameName = gameName.replaceAll("^\\u0000-\\u00FF", "");
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.getTimeStamp().get()));

		if (gameName.contains("+18")) {

			context.write(new Text(date.toString().concat("-1")), ones);

		} 
		else{
			context.write(new Text(date.toString().concat("-0")),ones);
		}
	}
}
