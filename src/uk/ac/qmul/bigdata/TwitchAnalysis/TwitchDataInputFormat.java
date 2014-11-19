package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class TwitchDataInputFormat extends FileInputFormat<Text,TwitchDataRecord> {


	public RecordReader<Text, TwitchDataRecord> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context) {
		return new TwitchRecordReader();
	}

	public class TwitchRecordReader extends RecordReader<Text, TwitchDataRecord> {
	
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;

		private Text key = null;
		private TwitchDataRecord value = null;

		private Text line = new Text();
		
		// internal fields for twitch data
		private IntWritable viewers = new IntWritable();
		private Text status = new Text();
		private Text displayName = new Text();
		private Text game = new Text();
		private IntWritable delay = new IntWritable();
		private Text user = new Text();
		private LongWritable timeStamp = new LongWritable();

		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;
			if (codec != null) {
				in = new LineReader(codec.createInputStream(fileIn), job);
				end = Long.MAX_VALUE;
			} else {
				if (start != 0) {
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new LineReader(fileIn, job);
			}
			if (skipFirstLine) { // skip first line and re-establish "start".
				start += in.readLine(new Text(), 0,
						(int) Math.min((long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
		}

		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new Text();
			}

			if (value == null) {
				value = new TwitchDataRecord();
			}
			int newSize = 0;

			while (pos < end) {
				newSize = in.readLine(line, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));
				if (newSize == 0) {
					break;
				}

				// fields:
				// tweetId, dateTime, hashTags, tweetBody 
				String[] fields = line.toString().split("\t");

				// data must be correctly formed
				if (fields == null || fields.length != 7) {
					break;
				}
				
				// parse key
				// key is hardcoded - not sure if that's right
				key.set("twitch-data");

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				// setting day by parsing from
				try {
					viewers.set(Integer.parseInt(fields[0]));
					status.set(fields[1]);
					displayName.set(fields[2]);
					game.set(fields[3]);
					delay.set(Integer.parseInt(fields[4]));
					user.set(fields[5]);
					timeStamp.set(sdf.parse(fields[6]).getTime());
					
					value.set(viewers, status, displayName, game, delay, user, timeStamp);

				} catch (ParseException e) {
					break;

				} catch (NumberFormatException e) {
					break;
				}
				//

				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}

				}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		@Override
		public Text getCurrentKey() {
			return key;
		}

		@Override
		public TwitchDataRecord getCurrentValue() {
			return value;
		}

		/**
		 * Get the progress within the split
		 */
		public float getProgress() {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		public synchronized void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}
	
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }

}

