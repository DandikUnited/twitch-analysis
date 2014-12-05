package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

import uk.ac.qmul.bigdata.TwitchAnalysis.TwitchDataRecord;

public class DailyTopGameMapper extends
Mapper<Object, TwitchDataRecord, Text, TextIntPair> {

	private Text data = new Text();

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.getTimeStamp().get()));
		String game = value.getGame().toString().toLowerCase().replaceAll("[^A-Za-z0-9]", "");
		if(game.length() > 0){
			TextIntPair gameViewers = new TextIntPair(game, value.getViewers().get()); 
			context.write(new Text(date), gameViewers);
		}


	}
}
