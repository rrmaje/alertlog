package com.plane.event.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "event_state")
public class EventState {

	public final static String FINISHED = "FINISHED";

	public final static String STARTED = "STARTED";

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long serialId;

	private String id;

	private String state;

	private long timestamp;

	@Column(nullable = true)
	private String type;

	@Column(nullable = true)
	private String host;

	@Column(nullable = true)
	private Integer bucket;

	public EventState() {

	}

	public EventState(String id, String state, long timestamp, String type, String host, int bucket) {
		super();
		this.id = id;
		this.state = state;
		this.timestamp = timestamp;
		this.type = type;
		this.host = host;
		this.bucket = bucket;
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

	public Integer getBucket() {
		return bucket;
	}

}
