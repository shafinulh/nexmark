/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink;

import static com.github.nexmark.flink.Benchmark.CATEGORY_OA;

import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.JobBenchmarkMetric;
import com.github.nexmark.flink.metric.MetricReporter;
import com.github.nexmark.flink.metric.Savepoint;
import com.github.nexmark.flink.workload.Workload;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryRunnerV2 {

	private static final Logger LOG = LoggerFactory.getLogger(QueryRunnerV2.class);
	private static final String ROCKSDB_STATS_DUMP_PERIOD_KEY =
			"state.backend.rocksdb.stats-dump-period-sec";
	private static final int DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SEC = 15;

	private final String queryName;
	private final Workload workload;
	private final Path location;
	private final Path queryLocation;
	private final Path flinkDist;
	private final MetricReporter metricReporter;
	private final FlinkRestClient flinkRestClient;
	private final int rocksdbStatsDumpPeriodSec;

	public QueryRunnerV2(String queryName, Workload workload, Path location, Path flinkDist, MetricReporter metricReporter, FlinkRestClient flinkRestClient, String category) {
		this.queryName = queryName;
		this.workload = workload;
		this.location = location;
		this.queryLocation =
				CATEGORY_OA.equals(category) ? location.resolve("queries") : location.resolve("queries-" + category);
		this.flinkDist = flinkDist;
		this.metricReporter = metricReporter;
		this.flinkRestClient = flinkRestClient;
		this.rocksdbStatsDumpPeriodSec = resolveRocksdbStatsDumpPeriodSec(flinkDist);
	}

	public JobBenchmarkMetric run() {
		try {
			Savepoint savepoint = null;
			System.out.println("==================================================================");
			System.out.println("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			LOG.info("==================================================================");
			LOG.info("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			long totalEvents = workload.getWarmupEvents() + workload.getEventsNum();
			System.out.println("Start the warmup for " + workload.getWarmupEvents() + " events.");
			LOG.info("Start the warmup for " + workload.getWarmupEvents() + " events.");
			String warmupJob = runWarmup(workload.getWarmupEvents(), totalEvents);
			long waited = waitForOrJobFinish(warmupJob, workload.getWarmupEvents());
			waited += waitForRocksdbStatsDumpWindow("warmup");
			Tuple2<Savepoint, Long> cancelResult = cancelJob(warmupJob, true);
			savepoint = cancelResult.f0;
			waited += cancelResult.f1;
			System.out.println("Stop the warmup, cost " + waited + "ms.");
			LOG.info("Stop the warmup, cost " + waited + ".");

			if (savepoint == null) {
				System.out.println("The query set warmup with savepoint, but does not get any savepoint.");
				LOG.error("The query set warmup with savepoint, but does not get any savepoint.");
				throw new RuntimeException("The query set warmup with savepoint, but does not get any savepoint.");
			} else {
				System.out.println("Get warmup savepoint: " + savepoint);
				LOG.info("Get warmup savepoint: " + savepoint);
			}


			String jobId = runInternal(totalEvents, savepoint);
			// blocking until collect enough metrics
			JobBenchmarkMetric metrics = metricReporter.reportMetric(jobId,
                    workload.getEventsNum(),
                    false,
                    workload.getKafkaServers() != null,
					Duration.ofSeconds(rocksdbStatsDumpPeriodSec));
			// cancel job
			System.out.println("Stop job query " + queryName);
			LOG.info("Stop job query " + queryName);
			cancelJob(jobId, false);
			return metrics;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private long waitForRocksdbStatsDumpWindow(String phaseName) {
		if (rocksdbStatsDumpPeriodSec <= 0) {
			LOG.info(
				"Skipping RocksDB post-{} wait because {}={} disables periodic stats dumps.",
				phaseName,
				ROCKSDB_STATS_DUMP_PERIOD_KEY,
				rocksdbStatsDumpPeriodSec);
			return 0L;
		}
		long waitMillis = rocksdbStatsDumpPeriodSec * 1000L;
		System.out.println(
			"Waiting " + rocksdbStatsDumpPeriodSec
				+ "s before " + phaseName
				+ " teardown to allow a final RocksDB stats dump.");
		LOG.info(
			"Waiting {} ms before {} teardown to allow a final RocksDB stats dump.",
			waitMillis,
			phaseName);
		try {
			Thread.sleep(waitMillis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(
				"Interrupted while waiting for RocksDB stats dump window after " + phaseName,
				e);
		}
		return waitMillis;
	}

	private static int resolveRocksdbStatsDumpPeriodSec(Path flinkDist) {
		try {
			Path flinkConfDir = flinkDist.resolve("conf");
			Configuration configuration = GlobalConfiguration.loadConfiguration(
				flinkConfDir.toString());
			return configuration.get(
				ConfigOptions.key(ROCKSDB_STATS_DUMP_PERIOD_KEY)
					.intType()
					.defaultValue(DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SEC));
		} catch (Throwable t) {
			LOG.warn(
				"Unable to determine {} from Flink configuration, using default {}s.",
				ROCKSDB_STATS_DUMP_PERIOD_KEY,
				DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SEC,
				t);
			return DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SEC;
		}
	}

	private long waitForOrJobFinish(String jobId, long recordLimit) {
		long start = System.currentTimeMillis();
		long lastRecords = -1;
		long lastChangeTime = System.currentTimeMillis();
		// Flink REST API metrics update at heartbeat intervals (seconds),
		// so we need a long idle window to avoid false positives.
		final long IDLE_TIMEOUT_MS = 30_000; // 30s of no metric change

		while (true) {
			long totalRecords = flinkRestClient.getTotalSourceReadRecords(jobId);
			if (totalRecords < 0) {
				break; // job not running
			}
			if (totalRecords >= recordLimit) {
				break; // threshold reached (works for single-source case)
			}
			// Track when metrics last changed
			if (totalRecords != lastRecords) {
				lastChangeTime = System.currentTimeMillis();
				lastRecords = totalRecords;
			}
			// Idle detection: if metrics haven't changed for IDLE_TIMEOUT_MS
			// and we have some records, sources are truly done.
			// (sum < recordLimit because not all event types are in the job graph)
			long idleMs = System.currentTimeMillis() - lastChangeTime;
			if (totalRecords > 0 && idleMs >= IDLE_TIMEOUT_MS) {
				LOG.info("Source write-records stable at {} for {}ms, "
						+ "treating warmup as complete (target was {}).",
						totalRecords, idleMs, recordLimit);
				System.out.println("Source write-records stable at " + totalRecords
						+ " for " + idleMs + "ms, treating warmup as complete (target was "
						+ recordLimit + ").");
				break;
			}
			try {
				Thread.sleep(1000L); // poll every 1s (metrics update slowly anyway)
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return System.currentTimeMillis() - start;
	}

	private Tuple2<Savepoint, Long> cancelJob(String jobId, boolean savepoint) {
		System.out.println("Stopping job " + jobId + " with savepoint = " + savepoint);
		long start = System.currentTimeMillis();
		boolean triggered = false;
		boolean savepointCompleted = false;
		String savepointPath = null;
		String requestId = null;

		while (!flinkRestClient.isJobCanceledOrFinished(jobId)) {
			if (savepoint) {
				if (!triggered) {
					requestId = flinkRestClient.stopWithSavepoint(jobId);
					triggered = true;
				} else if (!savepointCompleted) {
					Savepoint.Status status = flinkRestClient.checkSavepointFinished(jobId, requestId);
					if (status == Savepoint.Status.COMPLETED) {
						savepointCompleted = true;
						savepointPath = flinkRestClient.getSavepointLocation(jobId, requestId);
					} else if (status == Savepoint.Status.FAILED) {
						triggered = false;
					}
				}
			} else {
				flinkRestClient.cancelJob(jobId);
			}
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		if (savepoint && savepointPath == null && requestId != null) {
			long deadline = System.currentTimeMillis() + 60000L;
			while (System.currentTimeMillis() < deadline && savepointPath == null) {
				Savepoint.Status status = flinkRestClient.checkSavepointFinished(jobId, requestId);
				if (status == Savepoint.Status.COMPLETED) {
					savepointPath = flinkRestClient.getSavepointLocation(jobId, requestId);
					break;
				} else if (status == Savepoint.Status.FAILED) {
					break;
				}
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
		Savepoint result = null;
		if (savepoint) {
			if (savepointPath != null && !savepointPath.isEmpty()) {
				result = new Savepoint(Savepoint.Status.COMPLETED, savepointPath);
			} else {
				result = flinkRestClient.getJobLastCheckpoint(jobId);
			}
		}
		return Tuple2.of(result, System.currentTimeMillis() - start);
	}

	private String runWarmup(long stopAtEvents, long totalEvents) throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		varsMap.put("EVENTS_NUM", String.valueOf(totalEvents));
		varsMap.put("STOP_AT", String.valueOf(stopAtEvents));
		varsMap.put("KEEP_ALIVE", "true");
		List<String> sqlLines = initializeAllSqlLines(varsMap, queryName + "_warmup", null);
		return submitSQLJob(sqlLines);
	}

	private String runInternal(long totalEvents, Savepoint savepoint) throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		varsMap.put("EVENTS_NUM", String.valueOf(totalEvents));
		varsMap.put("STOP_AT", "-1");
		varsMap.put("KEEP_ALIVE", "true");
		List<String> sqlLines = initializeAllSqlLines(varsMap, queryName, savepoint);
		return submitSQLJob(sqlLines);
	}

	private Map<String, String> initializeVarsMap() {
		LocalDateTime currentTime = LocalDateTime.now();
		LocalDateTime submitTime = currentTime.minusNanos(currentTime.getNano());

		Map<String, String> varsMap = new HashMap<>();
		long baseTimeMillis = System.currentTimeMillis();
		varsMap.put("NEXMARK_DIR", location.toFile().getAbsolutePath());
		varsMap.put("SUBMIT_TIME", submitTime.toString());
		varsMap.put("BASE_TIME_MILLIS", String.valueOf(baseTimeMillis));
		varsMap.put("FLINK_HOME", flinkDist.toFile().getAbsolutePath());
		varsMap.put("TPS", String.valueOf(workload.getTps()));
		varsMap.put("PERSON_PROPORTION", String.valueOf(workload.getPersonProportion()));
		varsMap.put("AUCTION_PROPORTION", String.valueOf(workload.getAuctionProportion()));
		varsMap.put("BID_PROPORTION", String.valueOf(workload.getBidProportion()));
		varsMap.put("NEXMARK_TABLE", "datagen");
		varsMap.put("PERSON_TABLE", shouldReadUniqueKafkaTables() ? "person_kafka" : "person_src");
		varsMap.put("AUCTION_TABLE", shouldReadUniqueKafkaTables() ? "auction_kafka" : "auction_src");
		varsMap.put("BID_TABLE", shouldReadUniqueKafkaTables() ? "bid_kafka" : "bid_src");
		varsMap.put("BID_MODIFIED_TABLE", shouldReadUniqueKafkaTables() ? "bid_kafka" : "bid_modified_src");
		varsMap.put("BOOTSTRAP_SERVERS", workload.getKafkaServers() == null ? "" : workload.getKafkaServers());
		varsMap.put("MAX_EMIT_SPEED", isKafkaPrepareQuery() ? "false" : "true");
		varsMap.put("OCCASIONAL_DELAY_MIN_SEC", String.valueOf(workload.getOccasionalDelayMinSec()));
		varsMap.put("OCCASIONAL_DELAY_SEC", String.valueOf(workload.getOccasionalDelaySec()));
		varsMap.put("PROB_DELAYED_EVENT", String.valueOf(workload.getProbDelayedEvent()));
		varsMap.put("OUT_OF_ORDER_GROUP_SIZE", String.valueOf(workload.getOutOfOrderGroupSize()));
		varsMap.put("NUM_IN_FLIGHT_AUCTIONS", String.valueOf(workload.getNumInFlightAuctions()));
		return varsMap;
	}

	private List<String> initializeAllSqlLines(Map<String, String> vars, String name, Savepoint savepoint) throws IOException {
		List<String> allLines = new ArrayList<>();
		if (savepoint != null) {
			allLines.add("SET 'execution.savepoint.path' = '" + savepoint.getPath() + "';");
			allLines.add("SET 'execution.state-recovery.claim-mode' = 'CLAIM';");
		}
		if (name != null && !name.isEmpty()) {
			allLines.add("SET 'pipeline.name' = '" + name + "';");
		}
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), getDatagenDdlFile())));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_kafka.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), getViewsDdlFile())));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), queryName + ".sql")));
		return allLines;
	}

	private String getDatagenDdlFile() {
		return isUniqueQuery() ? "ddl_gen_unique_v2.sql" : "ddl_gen_v2.sql";
	}

	private String getViewsDdlFile() {
		return isUniqueQuery() ? "ddl_views_unique.sql" : "ddl_views.sql";
	}

	private boolean isUniqueQuery() {
		return queryName.endsWith("_unique");
	}

	private boolean isKafkaPrepareQuery() {
		return queryName.startsWith("insert_kafka");
	}

	private boolean shouldReadUniqueKafkaTables() {
		return isUniqueQuery() && workload.getKafkaServers() != null && !isKafkaPrepareQuery();
	}

	private List<String> initializeSqlFileLines(Map<String, String> vars, File sqlFile) throws IOException {
		List<String> lines = Files.readAllLines(sqlFile.toPath());
		List<String> result = new ArrayList<>();
		for (String line : lines) {
			for (Map.Entry<String, String> var : vars.entrySet()) {
				line = line.replace("${" + var.getKey() + "}", var.getValue());
			}
			result.add(line);
		}
		return result;
	}

	public String submitSQLJob(List<String> sqlLines) throws IOException {
		Path flinkBin = flinkDist.resolve("bin");
		LOG.info("\n================================================================================"
				+ "\nQuery {} is running."
				+ "\n--------------------------------------------------------------------------------"
				+ "\n"
			, queryName);

		Path sqlFile = Files.createTempFile("nexmark-sql-submit-", ".sql");
		Files.write(sqlFile, sqlLines, StandardCharsets.UTF_8);

		StringBuilder output = new StringBuilder();
		int exitCode;
		try {
			Process process = new ProcessBuilder(
					flinkBin.resolve("sql-client.sh").toAbsolutePath().toString(),
					"embedded",
					"-f",
					sqlFile.toAbsolutePath().toString())
					.redirectErrorStream(true)
					.start();

			try (BufferedReader reader = new BufferedReader(
					new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
				String line;
				while ((line = reader.readLine()) != null) {
					output.append(line).append(System.lineSeparator());
				}
			}

			try {
				exitCode = process.waitFor();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while waiting for sql-client.sh to finish.", e);
			}
		} finally {
			Files.deleteIfExists(sqlFile);
		}

		if (exitCode != 0) {
			throw new IOException("sql-client.sh failed with exit code "
					+ exitCode + ". Output:\n" + output);
		}

		Pattern pattern = Pattern.compile("Job ID: ([A-Za-z0-9]{32})");
		Matcher matcher = pattern.matcher(output.toString());
		if (matcher.find()) {
			return matcher.group(1);
		} else {
			throw new RuntimeException("Cannot find Job ID from the sql client output. Output:\n" + output);
		}
	}

}
