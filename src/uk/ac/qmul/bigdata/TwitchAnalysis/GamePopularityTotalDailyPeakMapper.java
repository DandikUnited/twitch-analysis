package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GamePopularityTotalDailyPeakMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private Text data = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] rowData = value.toString().split("\t");
		Text date = new Text(rowData[0]);
		IntWritable peakViewers = new IntWritable(Integer.parseInt(rowData[2]));
		

		context.write(date, peakViewers);

	}
}