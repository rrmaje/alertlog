package com.plane.event.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "event")
public class Event {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long serialId;

	private String id;

	private long duration;

	@Column(nullable = true)
	private String type;

	@Column(nullable = true)
	private String host;

	@Column(nullable = true)
	private boolean alert;

	public Event() {

	}

	public Event(String id, long duration, String type, String host, boolean alert) {
		super();
		this.id = id;
		this.duration = duration;
		this.type = type;
		this.host = host;
		this.alert = alert;
	}

	public boolean isAlert() {
		return alert;
	}

	public void setAlert(boolean alert) {
		this.alert = alert;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public String getType() {
		return type;
	}

	public String getHost() {
		return host;
	}

}
