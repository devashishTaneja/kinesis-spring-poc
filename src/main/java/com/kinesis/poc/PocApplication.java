package com.kinesis.poc;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.kinesis.poc.processor.RecordProcessorFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.CompletableFuture;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.*;

@SpringBootApplication
public class PocApplication implements CommandLineRunner {

	final RecordProcessorFactory recordProcessorFactory;

	public PocApplication(RecordProcessorFactory recordProcessorFactory) {
		this.recordProcessorFactory = recordProcessorFactory;
	}

	public static void main(String[] args) {
		SpringApplication.run(PocApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials("", "");
		KinesisClientLibConfiguration consumerConfig = new KinesisClientLibConfiguration(
			"KinesisKCLConsumer",
			"test_stream",
			"http://localhost:4567",
			"http://localhost:8000",
			DEFAULT_INITIAL_POSITION_IN_STREAM,
			new AWSStaticCredentialsProvider(basicAWSCredentials),
			new AWSStaticCredentialsProvider(basicAWSCredentials),
			new AWSStaticCredentialsProvider(basicAWSCredentials),
			DEFAULT_FAILOVER_TIME_MILLIS,
			"KinesisKCLConsumer",
			DEFAULT_MAX_RECORDS,
			DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
			DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
			DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
			DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
			DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
			new ClientConfiguration(),
			new ClientConfiguration(),
			new ClientConfiguration(),
			DEFAULT_TASK_BACKOFF_TIME_MILLIS,
			DEFAULT_METRICS_BUFFER_TIME_MILLIS,
			DEFAULT_METRICS_MAX_QUEUE_SIZE,
			DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
			Regions.US_EAST_1.getName(),
			DEFAULT_SHUTDOWN_GRACE_MILLIS,
			DEFAULT_DDB_BILLING_MODE,
			null,
			0,
			0,
			0
		);

		final Worker worker = new Worker.Builder()
				.recordProcessorFactory(recordProcessorFactory)
				.config(consumerConfig)
				.build();

		CompletableFuture.runAsync(() -> worker.run());
	}
}
