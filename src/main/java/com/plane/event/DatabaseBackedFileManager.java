package com.plane.event;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plane.event.model.EventState;

/**
 * 
 * I/O intensive version
 *
 */
public class DatabaseBackedFileManager implements Runnable {

	static final Logger LOGGER = LoggerFactory.getLogger(DatabaseBackedFileManager.class);

	private String fileName;

	@Autowired
	private EventStateRepository eventStateRepository;

	@Autowired
	private EventRepository eventRepository;

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

					LOGGER.debug("Starting analyzing partition [{}], records: {}", (int) count / batchSize,
							records.size());

					executor.execute(new BucketizedMapper(threshold, (int) count / batchSize, buckets, eventRepository,
							eventStateRepository).withRecords(records));

					records = new ArrayList<EventState>();

				}

			}

			shutdownAndAwaitTermination(executor);

			// process unmatched event state records written to buckets in mapping stage
			if (count > 0) {
				PostProcessor postProcessor = new PostProcessor(threshold, -1, buckets, eventRepository,
						eventStateRepository);

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

	long postProcessingCount() {
		return eventRepository.count();
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
