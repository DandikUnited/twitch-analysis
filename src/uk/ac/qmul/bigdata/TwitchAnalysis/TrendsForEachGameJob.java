package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import uk.ac.qmul.bigdata.TwitchAnalysis.TwitchDataInputFormat;

public class TrendsForEachGameJob {
	public static void runJob(String[] input, String output) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(TrendsForEachGameJob.class);
		job.setMapperClass(TrendsForEachGameMapper.class);
		job.setCombinerClass(TrendsForEachGameReducer.class);
		job.setReducerClass(TrendsForEachGameReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TwitchDataInputFormat.class);
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(",", input));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		runJob(Arrays.copyOfRange(args, 0, args.length - 1),
				args[args.length - 1]);
	}
}
