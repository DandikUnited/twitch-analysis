package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StreamLengthReducer extends	Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result;

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

					int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result = new IntWritable(sum);		
		context.write(key, result);	
	}

}




//Reducer<Text, LongWritable, Text, Text> {

/*public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		//long diffSeconds=0,diffMinutes = 0,diffHours=0,diffDays=0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dStart = null, dStop= null;
		String dateStart = "1989-12-09 00:00:00";
		String dateStop = "2015-12-09 00:00:00";
		try {
			dStart =sdf.parse(dateStart);
			dStop = sdf.parse(dateStop);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		for(LongWritable v: values){

			try {
				dStart =sdf.parse(dateStart);
				dStop = sdf.parse(dateStop);
			} catch (ParseException e2) {
				e2.printStackTrace();
			}

			Date date = new Date(v.get());
			String StrDate = sdf.format(date);
			try {
				date = sdf.parse(StrDate);
				dStart =sdf.parse(dateStart);
				dStop = sdf.parse(dateStop);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			if(date.after(dStart)){
				dStart = date;
			}
		}

		for(LongWritable v: values){
			try {
				dStart =sdf.parse(dateStart);
				dStop = sdf.parse(dateStop);
			} catch (ParseException e2) {
				e2.printStackTrace();
			}

			Date date = new Date(v.get());
			String StrDate = sdf.format(date);
			try {
				date = sdf.parse(StrDate);
				dStart =sdf.parse(dateStart);
				dStop = sdf.parse(dateStop);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			if(date.before(dStop)){
				dStop = date;
			}
		}



		Text streamDuration = new Text(dStart+" "+dStop);
		context.write(key, streamDuration);


	}
}*/


//extends
