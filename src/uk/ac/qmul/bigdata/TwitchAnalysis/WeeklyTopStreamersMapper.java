package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WeeklyTopStreamersMapper extends Mapper<Object, TwitchDataRecord, Text, TextIntPair> { 
	private Text user = new Text();
	private IntWritable viewers = new IntWritable();
	private TextIntPair UserViewers = new TextIntPair();
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-W");
	public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
		
		//getting the week of the stream
		String stringDate = formatter.format(new Date(value.getTimeStamp().get()));
		if(stringDate.endsWith("0")){
			stringDate = stringDate.substring(0,stringDate.length()-1) + "1";
		}
		
		//user
		user = value.getUser();
		if(value.getViewers().get() > 0){
			//viewers
			viewers = value.getViewers();
			UserViewers.set(user,viewers);
			//write date and a tuple(user,viewers)
			context.write(new Text(stringDate), UserViewers);
		}
	}
}