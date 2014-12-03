package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GamePopularityMaper extends
		Mapper<Object, TwitchDataRecord, Text, IntWritable> {

	private Text data = new Text();

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String gameName = value.getGame().toString();
		
		String[] gameFilters = context.getConfiguration().get("gameFilter").split(" ");
		
		for(int i=0; i<gameFilters.length; i++)
		{
			if(!gameName.toLowerCase().contains(gameFilters[i].toLowerCase()))
			{
				return;
			}
		}
		
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value
				.getTimeStamp().get()));
		
		context.write(new Text(date + "\t" + value.getUser()), value.getViewers());

	}
}