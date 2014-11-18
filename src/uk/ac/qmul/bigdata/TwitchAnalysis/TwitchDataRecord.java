package uk.ac.qmul.bigdata.TwitchAnalysis;

public class TwitchDataRecord {
	int viewers;
	String status;
	String display_name;
	String game;
	int delay;
	String user;
	String timeStamp;

	public TwitchDataRecord(int w, String s, String disp, String game, int d,
			String user, String ts) {
		viewers = w;
		status = s;
		display_name = disp;
		this.game = game;
		delay = d;
		this.user = user;
		timeStamp = ts;

	}

	public int getViewers() {
		return viewers;
	}

	public void setViewers(int viewers) {
		this.viewers = viewers;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getDisplay_name() {
		return display_name;
	}

	public void setDisplay_name(String display_name) {
		this.display_name = display_name;
	}

	public String getGame() {
		return game;
	}

	public void setGame(String game) {
		this.game = game;
	}

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

}
