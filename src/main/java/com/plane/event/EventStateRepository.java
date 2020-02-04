package com.plane.event;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.plane.event.model.EventState;

public interface EventStateRepository extends CrudRepository<EventState, Long> {

	List<EventState> findByBucket(Integer bucket);

	long countByBucket(int bucket);

}
