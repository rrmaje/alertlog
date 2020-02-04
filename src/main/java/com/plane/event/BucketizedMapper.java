package com.plane.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.plane.event.model.Event;
import com.plane.event.model.EventState;

class BucketizedMapper implements Runnable {

	private List<EventState> records = new ArrayList<>();

	EventRepository eventRepository;

	EventStateRepository eventStateRepository;

	int numberOfBuckets = 10;

	private int threshold = 4;

	private int id;

	public BucketizedMapper(int threshold, int id, int numberOfBuckets, EventRepository eventRepository,
			EventStateRepository eventStateRepository) {
		super();
		this.numberOfBuckets = numberOfBuckets;
		this.eventRepository = eventRepository;
		this.eventStateRepository = eventStateRepository;
		this.id = id;
		this.threshold = threshold;
	}

	public BucketizedMapper withRecords(List<EventState> records) {
		this.records = records;
		return this;
	}

	int bucketFor(EventState e) {
		int b = Math.abs(e.getId().hashCode()) % numberOfBuckets;
		DatabaseBackedFileManager.LOGGER.debug("Bucketizing id: {} to bucket: {}", e.getId(), b);
		return b;
	}

	@Override
	public void run() {

		try {
			List<EventState> unmatchedRecords = tryCollectAlerts();
			bucketizeRecords(unmatchedRecords);
		} catch (Exception e) {
			DatabaseBackedFileManager.LOGGER.error("Exception", e);
			return;
		}

	}

	List<EventState> tryCollectAlerts() throws Exception {
		List<Event> recordsForUpdate = new ArrayList<Event>();

		List<EventState> remainingRecords = new ArrayList<EventState>();

		Map<String, List<EventState>> eventIdToState = records.stream()
				.collect(Collectors.groupingBy(EventState::getId));

		eventIdToState.forEach((k, v) -> {
			EventState e1 = v.get(0);
			if (v.size() == 2) {
				EventState e2 = v.get(1);

				long duration = Math.abs(e1.getTimestamp() - e2.getTimestamp());
				if (duration > threshold) {

					recordsForUpdate.add(new Event(e1.getId(), duration, e1.getType(), e1.getHost(), true));
				}
			} else {
				DatabaseBackedFileManager.LOGGER.debug("Partition [{}] - Record not combined in current batch, id: {}",
						id, e1.getId());
				remainingRecords.add(e1);
			}

		});

		long start = System.currentTimeMillis();

		DatabaseBackedFileManager.LOGGER.debug("Partition [{}] - Inserting {} alerts into database", id,
				recordsForUpdate.size());

		eventRepository.saveAll(recordsForUpdate);

		long end = System.currentTimeMillis();

		long count = eventRepository.count();

		DatabaseBackedFileManager.LOGGER.debug(
				"Partition [{}] - finished inserting in {}ms, current number of alerts: {}", id, end - start, count);

		return remainingRecords;

	}

	private void bucketizeRecords(List<EventState> remaining) throws Exception {

		if (remaining.size() > 0) {
			DatabaseBackedFileManager.LOGGER.debug("Partition [{}] - Inserting pending {} records.", id,
					remaining.size());

			long start = System.currentTimeMillis();

			Map<Integer, List<EventState>> bucketToEventState = remaining.stream()
					.collect(Collectors.groupingBy(i -> bucketFor(i)));

			bucketToEventState.forEach((k, v) -> {
				long count = 0;
				List<EventState> recordsForUpdate = new ArrayList<EventState>();
				v.forEach(e -> {

					recordsForUpdate.add(
							new EventState(e.getId(), e.getState(), e.getTimestamp(), e.getHost(), e.getType(), k));
				});
				try {
					batchUpdate(recordsForUpdate);

					count = eventStateRepository.countByBucket(k);

					DatabaseBackedFileManager.LOGGER.debug(
							"Partition [{}]  - finished inserting to bucket {}, current number of pending records in bucket: {}.",
							id, k, count);

				} catch (Exception ex) {
					DatabaseBackedFileManager.LOGGER.error("Exception", ex);
					return;
				}

			});

			long end = System.currentTimeMillis();

			DatabaseBackedFileManager.LOGGER.debug("Partition [{}]  - finished inserting in {}ms.", id, end - start);

		}

	}

	private void batchUpdate(List<EventState> recordsForUpdate) throws Exception {
		eventStateRepository.saveAll(recordsForUpdate);
	}

}