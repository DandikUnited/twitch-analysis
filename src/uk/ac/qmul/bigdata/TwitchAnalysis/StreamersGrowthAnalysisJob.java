package uk.ac.qmul.bigdata.TwitchAnalysis;

//To delete the tmpU and out folders run this command: hadoop fs -rm -r tmpU && hadoop fs -rm -r out

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class StreamersGrowthAnalysisJob {
		public static void runJob(String[] input, String output) throws Exception {
			
			Configuration conf1 = new Configuration();
			Job job1 = new Job(conf1, "job1");

			Path tempPath = new Path("tmpU");
			Path outPath = new Path(output);
			outPath.getFileSystem(conf1).delete(outPath, true);
			tempPath.getFileSystem(conf1).delete(tempPath, true);
			job1.setJarByClass(StreamersGrowthAnalysisJob.class);
			job1.setMapperClass(StreamerFirstAppearenceMapper.class);
			job1.setCombinerClass(StreamerFirstAppearenceCombiner.class);
			job1.setReducerClass(StreamerFirstAppearenceReducer.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(LongWritable.class);

			job1.setInputFormatClass(TwitchDataInputFormat.class);
			
			FileInputFormat.setInputPaths(job1, StringUtils.join(",", input));
			FileOutputFormat.setOutputPath(job1, tempPath);
			
			
			Configuration conf2 = new Configuration();
			String[] otherArgs2 = new GenericOptionsParser(conf1, input)
					.getRemainingArgs();
			job1.waitForCompletion(true);
			
			Job job2 = new Job(conf2, "job2");

			job2.setJarByClass(StreamersGrowthAnalysisJob.class);
			job2.setMapperClass(StreamersGrowthMapper.class);
			job2.setCombinerClass(StreamersGrowthReducer.class);
			job2.setReducerClass(StreamersGrowthReducer.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			
			FileInputFormat.addInputPath(job2, tempPath);
			FileOutputFormat.setOutputPath(job2, outPath);
			
			job2.waitForCompletion(true);
		}

		public static void main(String[] args) throws Exception {
			runJob(Arrays.copyOfRange(args, 0, args.length - 1),
					args[args.length - 1]);
		}
}
