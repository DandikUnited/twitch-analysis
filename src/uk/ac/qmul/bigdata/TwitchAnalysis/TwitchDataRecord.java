package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TwitchDataRecord implements WritableComparable<TwitchDataRecord> {
	private IntWritable viewers;
	private Text status;
	private Text displayName;
	private Text game;
	private IntWritable delay;
	private Text user;
	private LongWritable timeStamp;

	public TwitchDataRecord() {
		set(new IntWritable(), new Text(), new Text(), new Text(),
				new IntWritable(), new Text(), new LongWritable());
	}

	public TwitchDataRecord(IntWritable viewers, Text status, Text displayName,
			Text game, IntWritable delay, Text user, LongWritable timeStamp) {
		set(viewers, status, displayName, game, delay, user, timeStamp);
	}
	
	public TwitchDataRecord(int viewers, String status, String displayName,
			String game, int delay, String user, long timeStamp) {
		set(new IntWritable(viewers), new Text(status), new Text(displayName),
				new Text(game), new IntWritable(delay), new Text(user),
				new LongWritable(timeStamp));
	}

	public void set(IntWritable viewers, Text status, Text displayName,
			Text game, IntWritable delay, Text user, LongWritable timeStamp) {
		this.viewers = viewers;
		this.status = status;
		this.displayName = displayName;
		this.game = game;
		this.delay = delay;
		this.user = user;
		this.timeStamp = timeStamp;
	}

	public IntWritable getViewers() {
		return viewers;
	}

	public void setViewers(IntWritable viewers) {
		this.viewers = viewers;
	}

	public Text getStatus() {
		return status;
	}

	public void setStatus(Text status) {
		this.status = status;
	}

	public Text getDisplayName() {
		return displayName;
	}

	public void setDisplayName(Text displayName) {
		this.displayName = displayName;
	}

	public Text getGame() {
		return game;
	}

	public void setGame(Text game) {
		this.game = game;
	}

	public IntWritable getDelay() {
		return delay;
	}

	public void setDelay(IntWritable delay) {
		this.delay = delay;
	}

	public Text getUser() {
		return user;
	}

	public void setUser(Text user) {
		this.user = user;
	}

	public LongWritable getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(LongWritable timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		viewers.write(out);
		status.write(out);
		displayName.write(out);
		game.write(out);
		delay.write(out);
		user.write(out);
		timeStamp.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		viewers.readFields(in);
		status.readFields(in);
		displayName.readFields(in);
		game.readFields(in);
		delay.readFields(in);
		user.readFields(in);
		timeStamp.readFields(in);

	}

	@Override
	public String toString() {
		return this.getViewers()
				+ "\t"
				+ this.getStatus()
				+ "\t"
				+ this.getDisplayName()
				+ "\t"
				+ this.getGame()
				+ "\t"
				+ this.getDelay()
				+ "\t"
				+ this.getUser()
				+ "\t"
				+ new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(
						this.timeStamp.get()));
	}

	// comparing/sorting on timestamp for each user
	@Override
	public int compareTo(TwitchDataRecord tweetData) {
		int cmp = this.getUser().compareTo(tweetData.user);
		if (cmp != 0) {
			return cmp;
		}
		return this.timeStamp.compareTo(tweetData.getTimeStamp());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((this.viewers == null) ? 0 : this.viewers.hashCode());
		result = prime * result
				+ ((this.status == null) ? 0 : this.status.hashCode());
		result = prime
				* result
				+ ((this.displayName == null) ? 0 : this.displayName.hashCode());
		result = prime * result
				+ ((this.game == null) ? 0 : this.game.hashCode());
		result = prime * result
				+ ((this.delay == null) ? 0 : this.delay.hashCode());
		result = prime * result
				+ ((this.user == null) ? 0 : this.user.hashCode());
		result = prime * result
				+ ((this.timeStamp == null) ? 0 : this.timeStamp.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TwitchDataRecord) {
			TwitchDataRecord st = (TwitchDataRecord) obj;
			return this.user.equals(st.getUser())
					&& this.timeStamp.equals(st.getTimeStamp()); // id is unique
																	// in
																	// twitter
		}
		return false;
	}

}
