package com.plane.event;

import org.springframework.data.repository.CrudRepository;

import com.plane.event.model.Event;

public interface EventRepository extends CrudRepository<Event, Long> {

}
