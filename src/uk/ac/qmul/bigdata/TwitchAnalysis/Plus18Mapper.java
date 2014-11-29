package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.*;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Plus18Mapper extends
Mapper<Object, TwitchDataRecord, Text, IntWritable> {

	private final IntWritable ones = new IntWritable(1);
	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {

		String gameName = value.getStatus().toString().toLowerCase();
		gameName = Normalizer.normalize(gameName, Normalizer.Form.NFD);
		gameName = gameName.replaceAll("^\\u0000-\\u00FF", "");
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(value.getTimeStamp().get()));
		
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
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		//sb.toString().contains("fcacfc7a5aef79126409a07ecc71ad72")
		if (gameName.contains("+18")) {
			context.write(new Text(date), ones);

		} 

	}
}
