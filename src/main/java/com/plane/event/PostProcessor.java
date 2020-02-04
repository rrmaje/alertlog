package com.plane.event;

import java.util.List;

import com.plane.event.model.EventState;

class PostProcessor extends BucketizedMapper {

	public PostProcessor(int threshold, int id, int numberOfBuckets, EventRepository eventRepository,
			EventStateRepository eventStateRepository) {
		super(threshold, id, numberOfBuckets, eventRepository, eventStateRepository);

	}

	public void run() {

		try {
			for (int i = 0; i < numberOfBuckets; i++) {
				List<EventState> records = eventStateRepository.findByBucket(i);
				DatabaseBackedFileManager.LOGGER.debug("Bucket {}, size: {}", i, records.size());
				withRecords(records);
				tryCollectAlerts();
			}
		} catch (Exception e) {
			DatabaseBackedFileManager.LOGGER.error("Exception: ", e);
			return;
		}

	}

}