package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopStreamersMapper extends Mapper<Object, TwitchDataRecord, Text, TextIntPair> { 
	private Text user = new Text();
	private IntWritable viewers = new IntWritable();
	private TextIntPair UserViewers = new TextIntPair();
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
		Text textDate = new Text();
		textDate.set(formatter.format(new Date(value.getTimeStamp().get())));
		user = value.getUser();
		if(value.getViewers().get() > 0){
			viewers = value.getViewers();
			UserViewers.set(user,viewers);
			
			context.write(textDate, UserViewers);
		}
	}
}