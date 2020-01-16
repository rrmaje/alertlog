package com.plane.event;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plane.event.alert.AlertLogApplication;

@SpringBootTest(classes = AlertLogApplication.class)
public class DatabaseBackedFileManagerTests {

	private static final String FILE_NAME = "eventlog.json.0";

	private static final int ALERTS_SIZE = 500_000;

	@Autowired
	private DatabaseBackedFileManager fileManager;

	@Test
	@Order(1)
	void givenEventStates_WhenThresholdIsExceededByEveryEvent_ThenAllEventsGenerateAlerts() throws Exception {

		fileManager.withBuckets(10).withFileName(FILE_NAME).withBatchSize(200_000).withMaxLength(2 * ALERTS_SIZE)
				.withThreshold(-1);

		fileManager.run();

		assertEquals(ALERTS_SIZE, fileManager.postProcessingCount());

	}

	@Test
	@Order(2)
	void givenEventStates_WhenThresholdNotExceeded_ThenAlertNotCreated() throws Exception {

		
		fileManager.recreateSchema();
		
		fileManager.withBuckets(10).withFileName(FILE_NAME).withBatchSize(200_000).withMaxLength(2 * ALERTS_SIZE)
				.withThreshold(100_000);

		fileManager.run();

		assertEquals(0, fileManager.postProcessingCount());

	}

	@BeforeAll
	public static void setUp() throws IOException {

		try {
			Files.deleteIfExists(Paths.get(FILE_NAME));
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			File f = new File(FILE_NAME);
			f.createNewFile();
		} catch (Exception e) {
			fail("Exception: ", e);
		}

		RandomAccessFile stream = new RandomAccessFile(FILE_NAME, "rw");
		FileChannel channel = stream.getChannel();

		final ObjectMapper mapper = new ObjectMapper();
		final String eType = "537597345872357359735735973485734753849753723987";
		final String eHost = "573485739457823907539753498753485737535723458735";

		ByteBuffer buffer = ByteBuffer.allocate(1024);

		long bytesWritten = IntStream.range(0, ALERTS_SIZE + 1).mapToLong(i -> {

			int b = 0;

			try {

				if (i == 0) {
					buffer.put(mapper.writeValueAsBytes(new EventState(String.valueOf(0), EventState.FINISHED,
							System.currentTimeMillis(), eType, eHost)));

					buffer.put("\n".getBytes());

					buffer.flip();
					b = channel.write(buffer);
				} else if (i == ALERTS_SIZE) {
					buffer.put(mapper.writeValueAsBytes(new EventState(String.valueOf(0), EventState.STARTED,
							System.currentTimeMillis(), eType, eHost)));

					buffer.put("\n".getBytes());

					buffer.flip();
					b = channel.write(buffer);
				} else {
					buffer.put(mapper.writeValueAsBytes(new EventState(String.valueOf(i), EventState.STARTED,
							System.currentTimeMillis(), eType, eHost)));

					buffer.put("\n".getBytes());

					buffer.put(mapper.writeValueAsBytes(new EventState(String.valueOf(i), EventState.FINISHED,
							System.currentTimeMillis() + 2, eType, eHost)));

					buffer.put("\n".getBytes());

					buffer.flip();
					b = channel.write(buffer);
				}

				buffer.clear();
				return b;

			} catch (Exception e) {
				fail("Exception: ", e);
				return 0;
			}

		}).sum();

		stream.close();
		channel.close();

		RandomAccessFile reader = new RandomAccessFile(FILE_NAME, "r");
		assertEquals(reader.length(), bytesWritten);
		reader.close();

	}

	@AfterAll
	public static void tearDown() {
		try {
			Files.deleteIfExists(Paths.get(FILE_NAME));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
