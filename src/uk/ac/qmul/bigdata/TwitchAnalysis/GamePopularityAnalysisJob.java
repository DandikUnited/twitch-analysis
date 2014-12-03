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

public class GamePopularityAnalysisJob {
	public static void runJob(String[] input, String output, String gameFilter) throws Exception {

		Configuration averageStreamConf = new Configuration();
		
		averageStreamConf.set("gameFilter", gameFilter);
		
		ControlledJob streamDailyPeakJob = new ControlledJob(averageStreamConf);
		streamDailyPeakJob.setJobName("Peak levels for each stream");

		Job job = streamDailyPeakJob.getJob();
		
		job.setJarByClass(GamePopularityAnalysisJob.class);
		job.setMapperClass(GamePopularityMaper.class);
		job.setCombinerClass(GamePopularityReducer.class);
		job.setReducerClass(GamePopularityReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TwitchDataInputFormat.class);
		
		Path outputPath = new Path(output);
		Path tempPath = new Path("tmp");
		
		FileInputFormat.setInputPaths(job, StringUtils.join(",", input));
		FileOutputFormat.setOutputPath(job, tempPath);
		
		outputPath.getFileSystem(averageStreamConf).delete(outputPath, true);
		tempPath.getFileSystem(averageStreamConf).delete(tempPath, true);

		
		job.waitForCompletion(true);

		Configuration dailyJobConfig = new Configuration();
		ControlledJob totalDailyPeakJob = new ControlledJob(dailyJobConfig);

		Job job2 = totalDailyPeakJob.getJob();
		totalDailyPeakJob.addDependingJob(streamDailyPeakJob);
		
		job2.setJarByClass(StreamerAndGameJob.class);
		job2.setMapperClass(GamePopularityTotalDailyPeakMapper.class);
		job2.setCombinerClass(GamePopularityTotalPeakReducer.class);
		job2.setReducerClass(GamePopularityTotalPeakReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(TextInputFormat.class);
				
		FileInputFormat.setInputPaths(job2, tempPath);
		FileOutputFormat.setOutputPath(job2, outputPath);
		
		job2.waitForCompletion(true);

		JobControl control = new JobControl("Game peak viewers for each day");
		control.addJob(streamDailyPeakJob);
		control.addJob(totalDailyPeakJob);
	
		
		control.run();
	}

	public static void main(String[] args) throws Exception {
		runJob(Arrays.copyOfRange(args, 0, args.length - 2),
				args[args.length - 2], args[args.length - 1]);
	}
}
