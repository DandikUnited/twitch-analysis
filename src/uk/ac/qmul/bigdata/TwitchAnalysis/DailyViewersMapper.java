package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyViewersMapper extends Mapper<Object, TwitchDataRecord, Text, IntWritable> {

	private Text data = new Text();

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String channelName = value.getUser().toString();

		String channelFilter = context.getConfiguration().get("channelFilter");

		if(!channelName.contains(channelFilter)){
			return;
		}

		Calendar c = Calendar.getInstance();
		Date streamTime = new Date(value.getTimeStamp().get());
		c.setTime(streamTime);
		
		int unroundedMinutes = c.get(Calendar.MINUTE);
		int mod = unroundedMinutes % 15;
		c.add(Calendar.MINUTE, mod < 8 ? -mod : (15-mod));
		
		Date roundedStreamTime = c.getTime();
		
		String dateAndHour = new SimpleDateFormat("dd-MMM-yyyy HH:mm").format(roundedStreamTime);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date referenceDate;
		try {
			referenceDate = sdf.parse(context.getConfiguration().get("dateFilter"));
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}
		
		
		c.setTime(referenceDate);
		
		c.add(Calendar.HOUR_OF_DAY, -12);
		Date dateBefore = c.getTime();
		
		c.setTime(referenceDate);
		c.add(Calendar.HOUR_OF_DAY, 36);
		Date dateAfter = c.getTime();
		
		
		if(roundedStreamTime.before(dateBefore) || roundedStreamTime.after(dateAfter)){
			return;
		}

		context.write(new Text(dateAndHour), value.getViewers());

	}
}