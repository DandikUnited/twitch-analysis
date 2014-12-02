package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class StreamerAndGameJob {
	public static void runJob(String[] input, String output) throws Exception {

		Configuration averageStreamConf = new Configuration();
		ControlledJob averageJob = new ControlledJob(averageStreamConf);
		averageJob.setJobName("Average viewers for stream + key composite key");

		Job job = averageJob.getJob();

		job.setJarByClass(StreamerAndGameJob.class);
		job.setMapperClass(StreamerAndGameMapper.class);
		job.setCombinerClass(StreamerAndGameCombiner.class);
		job.setReducerClass(StreamerAndGameReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntIntPair.class);
		job.setInputFormatClass(TwitchDataInputFormat.class);
		
		Path tempPath = new Path("tmp");
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(",", input));
		FileOutputFormat.setOutputPath(job, tempPath);
				
		outputPath.getFileSystem(averageStreamConf).delete(outputPath, true);
		tempPath.getFileSystem(averageStreamConf).delete(tempPath, true);

		
		job.waitForCompletion(true);

		Configuration topTenConfig = new Configuration();
		ControlledJob topTenJob = new ControlledJob(topTenConfig);

		Job job2 = topTenJob.getJob();
		topTenJob.addDependingJob(averageJob);
		
		job2.setJarByClass(StreamerAndGameJob.class);
		job2.setMapperClass(TopTenMapper.class);
		job2.setCombinerClass(TopTenCombiner.class);
		job2.setReducerClass(TopTenReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
				
		FileInputFormat.setInputPaths(job2, tempPath);
		FileOutputFormat.setOutputPath(job2, outputPath);
		
		job2.waitForCompletion(true);

		JobControl control = new JobControl("Top Streamers per game job");
		control.addJob(averageJob);
		control.addJob(topTenJob);
	
		
		control.run();
		
	}

	public static void main(String[] args) throws Exception {
		runJob(Arrays.copyOfRange(args, 0, args.length - 1),
				args[args.length - 1]);
	}
}
