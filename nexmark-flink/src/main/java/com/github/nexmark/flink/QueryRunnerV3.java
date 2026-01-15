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
import com.github.nexmark.flink.utils.AutoClosableProcess;
import com.github.nexmark.flink.workload.Workload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryRunnerV3 {

	private static final Logger LOG = LoggerFactory.getLogger(QueryRunnerV3.class);

	private final String queryName;
	private final Workload workload;
	private final Path location;
	private final Path queryLocation;
	private final Path flinkDist;
	private final MetricReporter metricReporter;
	private final FlinkRestClient flinkRestClient;
	private final String savepointPath;

	public QueryRunnerV3(String queryName, Workload workload, Path location, Path flinkDist, MetricReporter metricReporter, FlinkRestClient flinkRestClient, String category, String savepointPath) {
		this.queryName = queryName;
		this.workload = workload;
		this.location = location;
		this.queryLocation =
				CATEGORY_OA.equals(category) ? location.resolve("queries") : location.resolve("queries-" + category);
		this.flinkDist = flinkDist;
		this.metricReporter = metricReporter;
		this.flinkRestClient = flinkRestClient;
		this.savepointPath = savepointPath;
	}

	public JobBenchmarkMetric run() {
		if (savepointPath == null || savepointPath.isEmpty()) {
			throw new IllegalArgumentException("QueryRunnerV3 requires a savepoint path.");
		}
		try {
			System.out.println("==================================================================");
			System.out.println("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			System.out.println("Restoring from savepoint: " + savepointPath);
			LOG.info("==================================================================");
			LOG.info("Start to run query {} with workload {}", queryName, workload.getSummaryString());
			LOG.info("Restoring from savepoint: {}", savepointPath);

			long totalEvents = workload.getEventsNum();
			Savepoint savepoint = new Savepoint(Savepoint.Status.COMPLETED, savepointPath);
			String jobId = runInternal(totalEvents, savepoint);
			JobBenchmarkMetric metrics = metricReporter.reportMetric(
					jobId,
					workload.getEventsNum(),
					false,
					workload.getKafkaServers() != null);
			System.out.println("Stop job query " + queryName);
			LOG.info("Stop job query {}", queryName);
			cancelJob(jobId);
			return metrics;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void cancelJob(String jobId) {
		while (!flinkRestClient.isJobCanceledOrFinished(jobId)) {
			flinkRestClient.cancelJob(jobId);
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private String runInternal(long totalEvents, Savepoint savepoint) throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		varsMap.put("EVENTS_NUM", String.valueOf(totalEvents));
		varsMap.put("STOP_AT", "-1");
		varsMap.put("KEEP_ALIVE", "false");
		List<String> sqlLines = initializeAllSqlLines(varsMap, queryName, savepoint);
		return submitSQLJob(sqlLines);
	}

	private Map<String, String> initializeVarsMap() {
		LocalDateTime currentTime = LocalDateTime.now();
		LocalDateTime submitTime = currentTime.minusNanos(currentTime.getNano());

		Map<String, String> varsMap = new HashMap<>();
		varsMap.put("NEXMARK_DIR", location.toFile().getAbsolutePath());
		varsMap.put("SUBMIT_TIME", submitTime.toString());
		varsMap.put("FLINK_HOME", flinkDist.toFile().getAbsolutePath());
		varsMap.put("TPS", String.valueOf(workload.getTps()));
		varsMap.put("PERSON_PROPORTION", String.valueOf(workload.getPersonProportion()));
		varsMap.put("AUCTION_PROPORTION", String.valueOf(workload.getAuctionProportion()));
		varsMap.put("BID_PROPORTION", String.valueOf(workload.getBidProportion()));
		varsMap.put("NEXMARK_TABLE", "datagen");
		return varsMap;
	}

	private List<String> initializeAllSqlLines(Map<String, String> vars, String name, Savepoint savepoint) throws IOException {
		List<String> allLines = new ArrayList<>();
		if (savepoint != null) {
			allLines.add("SET 'execution.savepoint.path' = '" + savepoint.getPath() + "';");
			allLines.add("SET 'execution.state-recovery.claim-mode' = 'NO_CLAIM';");
		}
		if (name != null && !name.isEmpty()) {
			allLines.add("SET 'pipeline.name' = '" + name + "';");
		}
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_gen_v2.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_kafka.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_views.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), queryName + ".sql")));
		return allLines;
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
		final List<String> commands = new ArrayList<>();
		commands.add(flinkBin.resolve("sql-client.sh").toAbsolutePath().toString());
		commands.add("embedded");

		LOG.info("\n================================================================================"
				+ "\nQuery {} is running."
				+ "\n--------------------------------------------------------------------------------"
				+ "\n"
			, queryName);

		StringBuilder output = new StringBuilder();
		AutoClosableProcess
				.create(commands.toArray(new String[0]))
				.setStdInputs(sqlLines.toArray(new String[0]))
				.setStdoutProcessor(output::append) // logging the SQL statements and error message
				.runBlocking();

		Pattern pattern = Pattern.compile("Job ID: ([A-Za-z0-9]{32})");
		Matcher matcher = pattern.matcher(output.toString());
		if (matcher.find()) {
			return matcher.group(1);
		} else {
			throw new RuntimeException("Cannot find Job ID from the sql client output, maybe the job is not successfully submitted.");
		}
	}
}
