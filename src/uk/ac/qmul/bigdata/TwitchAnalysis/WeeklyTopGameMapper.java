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

public class WeeklyTopGameMapper extends
Mapper<Object, TwitchDataRecord, Text, TextIntPair> {

	private IntWritable viewers = new IntWritable();
	private TextIntPair UserViewers = new TextIntPair();
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-W");

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		Text game = new Text(value.getGame().toString().toLowerCase().replaceAll("[^A-Za-z0-9]", ""));
		//getting the week of the stream
		String stringDate = formatter.format(new Date(value.getTimeStamp().get()));
		if(stringDate.endsWith("0")){
			stringDate = stringDate.substring(0,stringDate.length()-1) + "1";
		}
		
		//user
		if(value.getViewers().get() > 0){
			//viewers
			viewers = value.getViewers();
			UserViewers.set(game,viewers);
			//write date and a tuple(user,viewers)
			context.write(new Text(stringDate), UserViewers);
		}

	}
}
