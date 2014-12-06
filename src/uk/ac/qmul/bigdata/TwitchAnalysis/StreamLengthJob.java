package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.IntWritable;

public class StreamLengthJob {
		public static void runJob(String[] input, String output) throws Exception {

			Configuration conf = new Configuration();

			Job job = new Job(conf);
			job.setJarByClass(StreamLengthJob.class);
			job.setMapperClass(StreamLengthMapper.class);
			job.setReducerClass(StreamLengthReducer.class);
			job.setCombinerClass(StreamLengthReducer.class);
			//job.setNumReduceTasks(5);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
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
