package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class DailyViewersChartJob {
	public static void runJob(String[] input, String output, String channelFilter, String dateFilter) throws Exception {

		Configuration dailyChartJobConf = new Configuration();
		
		dailyChartJobConf.set("channelFilter", channelFilter);
		dailyChartJobConf.set("dateFilter", dateFilter);

		
		ControlledJob streamDailyPeakJob = new ControlledJob(dailyChartJobConf);
		streamDailyPeakJob.setJobName("Peak levels for each stream");

		Job job = streamDailyPeakJob.getJob();
		
		job.setJarByClass(DailyViewersChartJob.class);
		job.setMapperClass(DailyViewersMapper.class);
		job.setCombinerClass(DailyViewersReducer.class);
		job.setReducerClass(DailyViewersReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TwitchDataInputFormat.class);
		
		Path outputPath = new Path(output);
		
		FileInputFormat.setInputPaths(job, StringUtils.join(",", input));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(dailyChartJobConf).delete(outputPath, true);

		
		
		
		job.waitForCompletion(true);

		JobControl control = new JobControl("Game peak viewers for each day");
		control.addJob(streamDailyPeakJob);
		
		control.run();
	}

	public static void main(String[] args) throws Exception {
		runJob(Arrays.copyOfRange(args, 0, args.length - 3),
				args[args.length - 3], args[args.length - 2], args[args.length - 1]);
	}
}