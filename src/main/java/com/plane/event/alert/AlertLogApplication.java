package com.plane.event.alert;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import com.plane.event.DatabaseBackedFileManager;

@SpringBootApplication
@EntityScan("com.plane.event.model")
@EnableJpaRepositories({ "com.plane.event" })
public class AlertLogApplication implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(AlertLogApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(AlertLogApplication.class, args);
	}

	@Autowired
	private DatabaseBackedFileManager fileManager;

	@Override
	public void run(String... args) throws Exception {

		LOGGER.debug("Running program with args: " + Arrays.toString(args));

		if (args.length != 1) {
			System.out.println("usage: java -jar <path to alertlog jar> <input-file>");

		} else {
			String fileName = args[0];

			fileManager.withFileName(fileName).withBatchSize(100_000).withBuckets(10).run();
		}

	}

	@Bean(name = "fileManager")
	public static DatabaseBackedFileManager create() {
		return new DatabaseBackedFileManager();
	}

}
