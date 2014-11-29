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

public class MonthlyPopularityMapper extends
		Mapper<Object, TwitchDataRecord, Text, IntWritable> {

	private Text data = new Text();

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value
				.getTimeStamp().get()));
		String[] dateArray = date.split("-");
		date = dateArray[1] +"-"+ dateArray[0];
		if(StringUtils.isAlphanumericSpace(value.getGame().toString())){
		String con = date + "\t" + value.getGame().toString().toLowerCase();
			context.write(new Text(con), value.getViewers());
		}

		

	}
}
