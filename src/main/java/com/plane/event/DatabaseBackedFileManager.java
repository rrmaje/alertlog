package com.plane.event;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * I/O intensive version
 *
 */
public class DatabaseBackedFileManager implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseBackedFileManager.class);

	private String fileName;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private int batchSize;

	private int maxLength;

	private int buckets = 10;

	private int threshold = 4;

	private int threadPoolSize = 2;

	@Override
	public void run() {

		LOGGER.debug("Reading input file [{}], batchSize: {}, maxLength: {}, threadPool: {}, threshold: {}", fileName,
				batchSize, maxLength, threadPoolSize, threshold);

		long start = System.currentTimeMillis();
		final ObjectMapper m = new ObjectMapper();
		long count = 0;

		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileName))));

			String line = null;

			List<EventState> records = new ArrayList<EventState>();

			ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

			while ((line = in.readLine()) != null && (maxLength <= 0 || count <= maxLength)) {
				count++;
				try {
					records.add(m.readValue(line, EventState.class));
				} catch (JsonProcessingException e) {
					LOGGER.error("Exception reading line: " + count + ", skipping...", e);
				}

				if (count % batchSize == 0) {

					LOGGER.debug("Starting analyzing partition [{}]", (int) count / batchSize);

					executor.execute(new BucketizedMapper(threshold, (int) count / batchSize, buckets, jdbcTemplate)
							.withRecords(records));

					records = new ArrayList<EventState>();
				}

			}

			shutdownAndAwaitTermination(executor);

			// process unmatched event state records written to buckets in mapping stage
			if (count > 0) {
				PostProcessor postProcessor = new PostProcessor(threshold, -1, buckets, jdbcTemplate);

				postProcessor.run();
			}

		} catch (Exception e) {
			LOGGER.error("Exception: ", e);
			return;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e2) {
					LOGGER.error("Exception: ", e2);
					return;
				}
			}
		}

		long end = System.currentTimeMillis();
		LOGGER.info("Processed {} records in {}ms", count, end - start);

	}

	public DatabaseBackedFileManager withThreadPool(int size) {
		this.threadPoolSize = size;
		return this;
	}

	public DatabaseBackedFileManager withBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public DatabaseBackedFileManager withThreshold(int threshold) {
		this.threshold = threshold;
		return this;
	}

	public DatabaseBackedFileManager withMaxLength(int maxLength) {
		this.maxLength = maxLength;
		return this;
	}

	public DatabaseBackedFileManager withBuckets(int b) {
		this.buckets = b;
		return this;
	}

	public DatabaseBackedFileManager withFileName(String fileName) {
		this.fileName = fileName;
		return this;
	}

	static class BucketizedMapper implements Runnable {

		private List<EventState> records = new ArrayList<>();

		int numberOfBuckets = 10;

		JdbcTemplate jdbcTemplate;

		private int threshold = 4;

		private int id;

		public BucketizedMapper(int threshold, int id, int numberOfBuckets, JdbcTemplate jdbcTemplate) {
			super();
			this.numberOfBuckets = numberOfBuckets;
			this.jdbcTemplate = jdbcTemplate;
			this.id = id;
			this.threshold = threshold;
		}

		public BucketizedMapper withRecords(List<EventState> records) {
			this.records = records;
			return this;
		}

		int bucketFor(EventState e) {
			int b = Math.abs(e.getId().hashCode()) % numberOfBuckets;
			LOGGER.debug("Bucketizing id: {} to bucket: {}", e.getId(), b);
			return b;
		}

		static String tableForBucket(int b) {
			return String.format("event_state_%d", b);
		}

		private static boolean initialized;

		private static Object lock = new Object();

		@Override
		public void run() {

			synchronized (lock) {
				if (!initialized) {
					recreateSchema();
					initialized = true;
				}
			}

			try {
				List<EventState> unmatchedRecords = tryCollectAlerts();
				bucketizeRecords(unmatchedRecords);
			} catch (Exception e) {
				LOGGER.error("Exception", e);
				return;
			}

		}

		List<EventState> tryCollectAlerts() throws Exception {
			List<Object[]> recordsForUpdate = new ArrayList<Object[]>();

			List<EventState> remainingRecords = new ArrayList<EventState>();

			Map<String, List<EventState>> eventIdToState = records.stream()
					.collect(Collectors.groupingBy(EventState::getId));

			eventIdToState.forEach((k, v) -> {

				EventState e1 = v.get(0);
				if (v.size() == 2) {
					EventState e2 = v.get(1);

					long duration = Math.abs(e1.getTimestamp() - e2.getTimestamp());

					if (duration > threshold) {
						recordsForUpdate.add(new Object[] { e1.getId(), e1.getHost(), e1.getType(), duration, true });
					}
				} else {
					LOGGER.debug("Partition [{}] - Record not combined in current batch, id: {}", id, e1.getId());
					remainingRecords.add(e1);
				}

			});

			long start = System.currentTimeMillis();

			LOGGER.debug("Partition [{}] - Inserting alerts into database", id);

			jdbcTemplate.batchUpdate("INSERT INTO event(id,host,type, duration, alert) VALUES (?,?,?,?,?)",
					recordsForUpdate);

			long end = System.currentTimeMillis();

			int count = jdbcTemplate.queryForObject("SELECT count(*) FROM event", Integer.class);

			LOGGER.debug("Partition [{}] - finished inserting in {}ms, current number of alerts: {}", id, end - start,
					count);

			return remainingRecords;

		}

		private void bucketizeRecords(List<EventState> remaining) throws Exception {

			if (remaining.size() > 0) {
				LOGGER.debug("Partition [{}] - Inserting pending {} records.", id, remaining.size());

				long start = System.currentTimeMillis();

				Map<Integer, List<EventState>> buckeToEventState = remaining.stream()
						.collect(Collectors.groupingBy(i -> bucketFor(i)));

				buckeToEventState.forEach((k, v) -> {
					int count = 0;
					List<Object[]> recordsForUpdate = new ArrayList<Object[]>();
					v.forEach(e -> {
						recordsForUpdate.add(
								new Object[] { e.getId(), e.getState(), e.getHost(), e.getType(), e.getTimestamp() });
					});

					try {
						batchUpdate(k, recordsForUpdate);

						count = jdbcTemplate.queryForObject("SELECT count(*) FROM " + tableForBucket(k), Integer.class);

						LOGGER.debug(
								"Partition [{}]  - finished inserting to bucket {}, current number of pending records in bucket: {}.",
								id, k, count);

					} catch (Exception e) {
						LOGGER.error("Exception", e);
						return;
					}

				});

				long end = System.currentTimeMillis();

				LOGGER.debug("Partition [{}]  - finished inserting in {}ms.", id, end - start);
			}

		}

		private void batchUpdate(int bucket, List<Object[]> recordsForUpdate) throws Exception {
			jdbcTemplate.batchUpdate(
					"INSERT INTO " + tableForBucket(bucket) + "(id,status,host,type,timestamp) VALUES (?,?,?,?,?)",
					recordsForUpdate);
		}

		void recreateSchema() {

			LOGGER.debug("Creating tables");

			for (int i = 0; i < numberOfBuckets; i++) {
				jdbcTemplate.execute("DROP TABLE " + tableForBucket(i) + " IF EXISTS");
				jdbcTemplate.execute("CREATE TABLE " + tableForBucket(i) + "("
						+ "serial_id INTEGER IDENTITY PRIMARY KEY, id VARCHAR(255), status VARCHAR(50), host VARCHAR(50), type VARCHAR(50), timestamp BIGINT"
						+ ")");

			}

			jdbcTemplate.execute("DROP TABLE event IF EXISTS");
			jdbcTemplate.execute("CREATE TABLE event("
					+ "serial_id INTEGER IDENTITY PRIMARY KEY, id VARCHAR(255), host VARCHAR(50), type VARCHAR(50), duration INTEGER, alert BOOLEAN DEFAULT FALSE"
					+ ")");

		}

	}

	void recreateSchema() {
		new BucketizedMapper(0, 0, buckets, jdbcTemplate).recreateSchema();
	}

	static class PostProcessor extends BucketizedMapper {

		public PostProcessor(int threshold, int id, int numberOfBuckets, JdbcTemplate jdbcTemplate) {
			super(threshold, id, numberOfBuckets, jdbcTemplate);

		}

		public void run() {

			try {
				for (int i = 0; i < numberOfBuckets; i++) {
					List<EventState> records = this.jdbcTemplate.query("SELECT * FROM " + tableForBucket(i),
							(rs, rowNum) -> new EventState(rs.getString("id"), rs.getString("status"),
									rs.getLong("timestamp"), rs.getString("type"), rs.getString("host")));
					LOGGER.debug("Bucket {}, size: {}", i, records.size());
					withRecords(records);
					tryCollectAlerts();
				}
			} catch (Exception e) {
				LOGGER.error("Exception: ", e);
				return;
			}

		}

	}

	int postProcessingCount() {
		return jdbcTemplate.queryForObject("SELECT count(*) FROM event", Integer.class);
	}

	void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(600, TimeUnit.SECONDS)) {
				pool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(60, TimeUnit.SECONDS))
					LOGGER.error("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

}
