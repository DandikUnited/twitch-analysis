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

public class RivalAnalysisJob {
		public static void runJob(String[] input, String output) throws Exception {

			Configuration conf = new Configuration();

			Job job = new Job(conf);
			job.setJarByClass(RivalAnalysisJob.class);
			job.setMapperClass(RivalMapper.class);
			job.setReducerClass(RivalReducerPerChanell.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setInputFormatClass(TwitchDataInputFormat.class);
			
			Path outputPath = new Path(output);
			FileInputFormat.setInputPaths(job, StringUtils.join(",", input));
			FileOutputFormat.setOutputPath(job, outputPath);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			job.waitForCompletion(true);
		}
		public static void runJob2(String[] input, String output) throws Exception {

			Configuration conf = new Configuration();

			Job job = new Job(conf, "Job1");
			job.setJarByClass(RivalAnalysisJob.class);
			job.setJarByClass(RivalAnalysisJob.class);
			job.setMapperClass(RivalMapperBridge.class);
			job.setReducerClass(RivalReducer.class);
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
