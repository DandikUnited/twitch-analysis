package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RivalAnalysisMapperTwo extends Mapper<Object, Object, Text, IntWritable>{
	

	public void map(Object key, Object value, Context context)
			throws IOException, InterruptedException {
		String[] bla = value.toString().split("\t");
		int foo = Integer.parseInt(bla[2]);
		IntWritable go = new IntWritable(foo);
		context.write(new Text(bla[0]+"\t"+bla[1]), go);

	}

}
