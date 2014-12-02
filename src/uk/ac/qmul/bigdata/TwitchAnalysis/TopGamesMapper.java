package uk.ac.qmul.bigdata.TwitchAnalysis;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopGamesMapper extends Mapper<Object, TwitchDataRecord, Text, IntWritable> { 
		private Text game = new Text();
		private IntWritable one = new IntWritable(1);
	    public void map(Object key, TwitchDataRecord value, Context context) throws IOException, InterruptedException {
	    	
	    	game.set(value.getGame().toString().toLowerCase().replaceAll("[^A-Za-z0-9]", ""));
	    	
	    	context.write(game, one);
	    }
}