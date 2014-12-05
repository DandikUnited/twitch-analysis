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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class RivalAnalysisJob {
	public static void runJob(String[] input, String output) throws Exception {

		Configuration conf1 = new Configuration();
		ControlledJob cJob1 = new ControlledJob(conf1);
		cJob1.setJobName("Peak levels");
		Job job1 = cJob1.getJob();

		
		Path tempPath = new Path("tmp");
		Path outPath = new Path(output);
		
		
		job1.setJarByClass(RivalAnalysisJob.class);
		job1.setMapperClass(RivalAnalysisMapperOne.class);
		job1.setReducerClass(RivalAnalysisReducerOne.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setInputFormatClass(TwitchDataInputFormat.class);
		FileInputFormat.addInputPaths(job1, StringUtils.join(",", input));
		FileOutputFormat.setOutputPath(job1, tempPath);

		Configuration conf2 = new Configuration();
		String[] otherArgs2 = new GenericOptionsParser(conf1, input)
				.getRemainingArgs();
		job1.waitForCompletion(true);
		

		ControlledJob cJob2 = new ControlledJob(conf2);
		cJob2.setJobName("finalizer");
		Job job2 = cJob2.getJob();
		cJob2.addDependingJob(cJob1);
		job2.setJarByClass(RivalAnalysisJob.class);
		job2.setMapperClass(RivalAnalysisMapperTwo.class);
		job2.setReducerClass(RivalAnalysisReducerTwo.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, tempPath);
		FileOutputFormat.setOutputPath(job2, outPath);
		
		//outPath.getFileSystem(conf1).delete(outPath, true);
		//tempPath.getFileSystem(conf1).delete(tempPath, true);
		
		job2.waitForCompletion(true);


		JobControl control = new JobControl("Rival Analysis");
		control.addJob(cJob1);
		control.addJob(cJob2);
		control.run();

	}

	public static void main(String[] args) throws Exception {
		runJob(Arrays.copyOfRange(args, 0, args.length - 1),
				args[args.length - 1]);
	}
}
