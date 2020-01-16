package com.plane.event;

public class EventState {

	public final static String FINISHED = "FINISHED";

	public final static String STARTED = "STARTED";

	private String id;

	private String state;

	private long timestamp;

	private String type;

	private String host;

	public EventState() {

	}

	public EventState(String id, String state, long timestamp, String type, String host) {
		super();
		this.id = id;
		this.state = state;
		this.timestamp = timestamp;
		this.type = type;
		this.host = host;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setHost(String host) {
		this.host = host;
	}


	public String getState() {
		return state;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getType() {
		return type;
	}

	public String getHost() {
		return host;
	}

}
