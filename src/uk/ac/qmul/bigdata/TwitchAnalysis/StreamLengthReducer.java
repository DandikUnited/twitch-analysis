package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class StreamLengthReducer extends
Reducer<Text, Text, Text, IntWritable> {
	//K2:GameName V2:DateTime
	private IntWritable streamDuration = new IntWritable(0);
	private Text channelGUID = new Text();


	public void reduce(Iterable<Text> key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, ParseException {

		SimpleDateFormat Format  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (Text k : key){
			Date streamStarted = null, streamEnded = null;
			Date dummyDate = Format.parse("12/09/1989") ;
			for (Text value : values ){
				try
				{
					Date date = Format.parse(value.toString());
					if(dummyDate.before(date)){
						streamEnded = date;
						dummyDate = date;
					}

					if(streamEnded.after(date)){
						streamStarted = date;


					}
				}
				catch (java.text.ParseException ex)
				{
					ex.printStackTrace();
				}



			}	
			long diff = (((streamEnded.getTime() - streamStarted.getTime())/ (1000*60)) % 60);


			streamDuration = new IntWritable((int)diff);
			channelGUID = new Text(k);

			context.write(channelGUID,streamDuration);
		}




	}

}
