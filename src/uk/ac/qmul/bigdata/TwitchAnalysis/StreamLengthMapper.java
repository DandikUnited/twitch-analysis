package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StreamLengthMapper extends
Mapper<Object, TwitchDataRecord, Text, Text> {

		public void map(Object key, TwitchDataRecord value, Context context)
				throws IOException, InterruptedException {
				String gameName = value.getStatus().toString().toLowerCase();
				gameName = Normalizer.normalize(gameName, Normalizer.Form.NFD);
				gameName = gameName.replaceAll("^\\u0000-\\u00FF", "");
				String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(value.getTimeStamp().get()));
				String original = gameName;
				StringBuffer sb = new StringBuffer();
				MessageDigest md;
				try {
				md = MessageDigest.getInstance("MD5");
				md.update(original.getBytes());
				byte[] digest = md.digest();
				for (byte b : digest) {
				sb.append(String.format("%02x", b & 0xff));
				}
				}catch (NoSuchAlgorithmException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					}

					context.write(new Text(date.toString()),new Text(sb.toString()));

					}
					}