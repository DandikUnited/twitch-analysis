package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RivalMapper extends
		Mapper<Object, TwitchDataRecord, Text, IntWritable> {

	private Text data = new Text();

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.getTimeStamp().get()));
		
		String con = value.getUser().toString() + "\t"
				+ date + "\t" + value.getGame();

		if (value.getGame().toString().toLowerCase().contains("league of")) {
			context.write(new Text(con), value.getViewers());

		} else if (value.getGame().toString().toLowerCase().contains("dota")) {
			context.write(new Text(con), value.getViewers());

		}

	}
}
