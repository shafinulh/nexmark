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

package com.github.nexmark.flink.sql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.nexmark.flink.source.NexmarkSourceOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Standalone entry point for executing Nexmark SQL queries directly from the shaded jar.
 *
 * <p>The implementation mirrors the logic used by {@code QueryRunner} when it feeds the SQL CLI,
 * but executes the statements programmatically so the job can be managed like any other Flink
 * application deployment.
 */
public final class SqlQueryJob {

	private static final Logger LOG = LoggerFactory.getLogger(SqlQueryJob.class);

	private SqlQueryJob() {
	}

	public static void main(String[] args) throws Exception {
		SqlJobOptions options = SqlJobOptions.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		options.getParallelism().ifPresent(env::setParallelism);

		TableEnvironment tableEnv = StreamTableEnvironment.create(env);
		options.getJobName()
			.ifPresent(name -> tableEnv.getConfig().getConfiguration().setString(PipelineOptions.NAME.key(), name));

		Map<String, String> variables = options.buildTemplateVariables();

		executeResourceStatements(tableEnv, "queries/ddl_gen.sql", variables);
		executeResourceStatements(tableEnv, "queries/ddl_kafka.sql", variables);
		executeResourceStatements(tableEnv, "queries/ddl_views.sql", variables);
		executeResourceStatements(tableEnv, options.getQueryResource(), variables);
	}

	private static void executeResourceStatements(
			TableEnvironment tableEnv, String resourcePath, Map<String, String> variables)
			throws IOException, InterruptedException, ExecutionException {
		List<String> statements = loadStatements(resourcePath, variables);
		for (String statement : statements) {
			if (statement.isEmpty()) {
				continue;
			}
			LOG.info("Executing statement: {}", statement);
			if (statement.regionMatches(true, 0, "INSERT", 0, "INSERT".length())) {
				tableEnv.executeSql(statement).await();
			} else {
				tableEnv.executeSql(statement);
			}
		}
	}

	private static List<String> loadStatements(String resourcePath, Map<String, String> variables) throws IOException {
		try (InputStream stream = SqlQueryJob.class.getClassLoader().getResourceAsStream(resourcePath)) {
			if (stream == null) {
				throw new IllegalArgumentException("Unable to locate SQL resource on classpath: " + resourcePath);
			}

			try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
				List<String> statements = new ArrayList<>();
				StringBuilder currentStatement = new StringBuilder();
				String line;
				while ((line = reader.readLine()) != null) {
					String substitutedLine = applyVariables(line, variables);
					String trimmed = substitutedLine.trim();
					if (trimmed.isEmpty() || trimmed.startsWith("--")) {
						continue;
					}
					currentStatement.append(substitutedLine).append('\n');
					if (trimmed.endsWith(";")) {
						statements.add(cleanStatement(currentStatement.toString()));
						currentStatement.setLength(0);
					}
				}
				if (currentStatement.length() > 0) {
					statements.add(cleanStatement(currentStatement.toString()));
				}
				return statements;
			}
		}
	}

	private static String applyVariables(String line, Map<String, String> variables) {
		String result = line;
		for (Map.Entry<String, String> entry : variables.entrySet()) {
			result = result.replace("${" + entry.getKey() + "}", entry.getValue());
		}
		return result;
	}

	private static String cleanStatement(String statement) {
		String trimmed = statement.trim();
		if (trimmed.endsWith(";")) {
			trimmed = trimmed.substring(0, trimmed.length() - 1);
		}
		return trimmed.trim();
	}

	private static final class SqlJobOptions {
		private final String query;
		private final String category;
		private final Optional<String> jobName;
		private final Optional<Integer> parallelism;
		private final long tps;
		private final long events;
		private final int personProportion;
		private final int auctionProportion;
		private final int bidProportion;
		private final Optional<String> bootstrapServers;
		private final String nexmarkDir;
		private final String flinkHome;

		private SqlJobOptions(
				String query,
				String category,
				Optional<String> jobName,
				Optional<Integer> parallelism,
				long tps,
				long events,
				int personProportion,
				int auctionProportion,
				int bidProportion,
				Optional<String> bootstrapServers,
				String nexmarkDir,
				String flinkHome) {
			this.query = query;
			this.category = category;
			this.jobName = jobName;
			this.parallelism = parallelism;
			this.tps = tps;
			this.events = events;
			this.personProportion = personProportion;
			this.auctionProportion = auctionProportion;
			this.bidProportion = bidProportion;
			this.bootstrapServers = bootstrapServers;
			this.nexmarkDir = nexmarkDir;
			this.flinkHome = flinkHome;
		}

		static SqlJobOptions fromArgs(String[] args) {
			ParameterTool params = ParameterTool.fromArgs(args);
			String query = params.getRequired("query");
			String category = params.get("category", "oa");
			Optional<String> jobName = firstPresent(params, "job-name", "jobName");
			Optional<Integer> parallelism = optionalInt(params, "parallelism");
			long tps = params.getLong("tps", NexmarkSourceOptions.NEXT_EVENT_RATE.defaultValue().longValue());
			long events = params.getLong("events", NexmarkSourceOptions.EVENTS_NUM.defaultValue());
			int personProportion = params.getInt("person-proportion", NexmarkSourceOptions.PERSON_PROPORTION.defaultValue());
			int auctionProportion = params.getInt("auction-proportion", NexmarkSourceOptions.AUCTION_PROPORTION.defaultValue());
			int bidProportion = params.getInt("bid-proportion", NexmarkSourceOptions.BID_PROPORTION.defaultValue());
			Optional<String> bootstrapServers = firstPresent(params, "bootstrap-servers", "kafka-bootstrap-servers");
			String nexmarkDir = params.get("nexmark-dir", System.getProperty("user.dir"));
			String flinkHome = params.get("flink-home", System.getenv().getOrDefault("FLINK_HOME", ""));

			return new SqlJobOptions(
					query,
					category,
					jobName,
					parallelism,
					tps,
					events,
					personProportion,
					auctionProportion,
					bidProportion,
					bootstrapServers,
					nexmarkDir,
					flinkHome);
		}

		Optional<String> getJobName() {
			return jobName;
		}

		Optional<Integer> getParallelism() {
			return parallelism;
		}

		Map<String, String> buildTemplateVariables() {
			Map<String, String> variables = new HashMap<>();
			variables.put("NEXMARK_DIR", nexmarkDir);
			LocalDateTime submitTime = LocalDateTime.now().withNano(0);
			variables.put("SUBMIT_TIME", submitTime.toString());
			variables.put("FLINK_HOME", flinkHome);
			variables.put("TPS", Long.toString(tps));
			variables.put("EVENTS_NUM", Long.toString(events));
			variables.put("PERSON_PROPORTION", Integer.toString(personProportion));
			variables.put("AUCTION_PROPORTION", Integer.toString(auctionProportion));
			variables.put("BID_PROPORTION", Integer.toString(bidProportion));
			boolean useKafka = bootstrapServers.filter(s -> !s.isEmpty()).isPresent();
			variables.put("NEXMARK_TABLE", useKafka ? "kafka" : "datagen");
			variables.put("BOOTSTRAP_SERVERS", bootstrapServers.orElse(""));
			return variables;
		}

		String getQueryResource() {
			String normalizedCategory = category.toLowerCase(Locale.ROOT);
			String basePath = "oa".equals(normalizedCategory)
					? "queries/"
					: "queries-" + normalizedCategory + "/";
			return basePath + query + ".sql";
		}

		private static Optional<String> firstPresent(ParameterTool params, String... keys) {
			for (String key : keys) {
				if (params.has(key)) {
					String value = params.get(key);
					if (value != null && !value.isEmpty()) {
						return Optional.of(value);
					}
				}
			}
			return Optional.empty();
		}

		private static Optional<Integer> optionalInt(ParameterTool params, String key) {
			if (!params.has(key)) {
				return Optional.empty();
			}
			return Optional.of(params.getInt(key));
		}
	}
}
