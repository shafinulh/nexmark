/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/** Entry point for running Nexmark SQL queries inside a Flink application cluster. */
public class SqlQueryJob {

	private static final Logger LOG = LoggerFactory.getLogger(SqlQueryJob.class);

	public static void main(String[] args) throws Exception {
		Options options = buildOptions();
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine;
		try {
			commandLine = parser.parse(options, args);
		} catch (Exception e) {
			printHelp(options);
			throw e;
		}

		if (commandLine.hasOption("help")) {
			printHelp(options);
			return;
		}

		QueryOptions queryOptions = QueryOptions.from(commandLine);
		List<String> statements =
				new NexmarkSqlScriptLoader()
						.loadStatements(
								queryOptions.queryName,
								queryOptions.usesKafkaSource(),
								queryOptions.toTemplateVariables());

		if (queryOptions.printScript) {
			LOG.info("Expanded Nexmark SQL script:\n{}", joinStatements(statements));
		}

		executeStatements(statements, queryOptions);
	}

	private static void executeStatements(List<String> statements, QueryOptions queryOptions)
			throws Exception {
		StreamExecutionEnvironment environment =
				StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
		Configuration tableConfiguration = tableEnvironment.getConfig().getConfiguration();

		if (queryOptions.jobName != null) {
			tableConfiguration.setString("pipeline.name", queryOptions.jobName);
		}
		if (queryOptions.savepointPath != null) {
			tableConfiguration.setString("execution.savepoint.path", queryOptions.savepointPath);
			tableConfiguration.setString("execution.state-recovery.claim-mode", "NO_CLAIM");
		}

		TableResult terminalResult = null;
		for (String statement : statements) {
			if (terminalResult != null) {
				throw new IllegalArgumentException(
						"Statements found after the terminal DML statement. "
								+ "Wrap multiple INSERT statements in EXECUTE STATEMENT SET.");
			}

			Matcher setStatementMatcher =
					SqlScriptParser.SET_STATEMENT_PATTERN.matcher(statement.trim());
			if (setStatementMatcher.matches()) {
				applySetStatement(setStatementMatcher, queryOptions, tableConfiguration);
				continue;
			}

			if (SqlScriptParser.isTerminalDml(statement)) {
				LOG.info("Submitting terminal SQL statement:\n{}", statement);
				terminalResult = tableEnvironment.executeSql(statement);
			} else {
				LOG.info("Executing setup SQL statement:\n{}", statement);
				tableEnvironment.executeSql(statement);
			}
		}

		if (terminalResult == null) {
			throw new IllegalArgumentException(
					"No INSERT or EXECUTE STATEMENT SET statement found for query "
							+ queryOptions.queryName);
		}

		logSubmittedJob(terminalResult, queryOptions.queryName);
		if (queryOptions.waitForFinish) {
			waitForJobCompletion(terminalResult, queryOptions.queryName);
		}
	}

	private static void applySetStatement(
			Matcher setStatementMatcher,
			QueryOptions queryOptions,
			Configuration tableConfiguration) {
		String key = setStatementMatcher.group(1);
		String value = setStatementMatcher.group(2);

		if (queryOptions.jobName != null && "pipeline.name".equals(key)) {
			LOG.info("Skipping pipeline.name from SQL resource because --job-name was provided.");
			return;
		}
		if (queryOptions.savepointPath != null && "execution.savepoint.path".equals(key)) {
			LOG.info(
					"Skipping execution.savepoint.path from SQL resource because --savepoint was provided.");
			return;
		}

		LOG.info("Applying SQL SET '{}'='{}'", key, value);
		tableConfiguration.setString(key, value);
	}

	private static void logSubmittedJob(TableResult tableResult, String queryName) {
		JobClient jobClient =
				tableResult
						.getJobClient()
						.orElseThrow(
								() ->
										new IllegalStateException(
												"No JobClient was returned for query " + queryName));
		LOG.info("Submitted Flink job {} for query {}", jobClient.getJobID(), queryName);
		LOG.info(
				"Query {} was submitted in application mode; the cluster lifecycle is now managed by Flink.",
				queryName);
	}

	private static void waitForJobCompletion(TableResult tableResult, String queryName)
			throws Exception {
		JobClient jobClient =
				tableResult
						.getJobClient()
						.orElseThrow(
								() ->
										new IllegalStateException(
												"No JobClient was returned for query " + queryName));
		LOG.info("Waiting for Flink job {} for query {} to finish.", jobClient.getJobID(), queryName);
		jobClient.getJobExecutionResult().get();
		LOG.info("Flink job {} for query {} finished.", jobClient.getJobID(), queryName);
	}

	private static Options buildOptions() {
		Options options = new Options();
		options.addOption(
				Option.builder()
						.longOpt("query")
						.hasArg()
						.required()
						.desc("Nexmark SQL query resource name, for example q20_unique")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("source")
						.hasArg()
						.desc("Source type: datagen or kafka. Defaults to datagen.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("bootstrap-servers")
						.hasArg()
						.desc("Kafka bootstrap servers, required when Kafka is used as a source or sink.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("tps")
						.hasArg()
						.desc("Event rate for the datagen source. Defaults to 10000.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("events")
						.hasArg()
						.desc("Number of events to emit. Defaults to 0 (unbounded).")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("person-proportion")
						.hasArg()
						.desc("Person event proportion. Defaults to 1.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("auction-proportion")
						.hasArg()
						.desc("Auction event proportion. Defaults to 3.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("bid-proportion")
						.hasArg()
						.desc("Bid event proportion. Defaults to 46.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("keep-alive")
						.hasArg()
						.desc("Whether the datagen source should stay alive after reaching the limit.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("stop-at")
						.hasArg()
						.desc("Stop at a specific event id. Defaults to -1.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("max-emit-speed")
						.hasArg()
						.desc("Whether to bypass event-time pacing in the Nexmark source.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("base-time-millis")
						.hasArg()
						.desc("Optional base event-time in epoch millis. Defaults to current time.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("num-in-flight-auctions")
						.hasArg()
						.desc("Number of in-flight auctions. Defaults to 100.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("job-name")
						.hasArg()
						.desc("Optional pipeline.name override.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("savepoint")
						.hasArg()
						.desc("Optional savepoint path for restoring the query.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("occasional-delay-min-sec")
						.hasArg()
						.desc(
								"Minimum occasional delay to impose on events, in seconds. Defaults to 60.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("occasional-delay-sec")
						.hasArg()
						.desc(
								"Maximum occasional delay to impose on events, in seconds. Defaults to 240.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("prob-delayed-event")
						.hasArg()
						.desc("Probability that an event will be delayed. Defaults to 0.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("out-of-order-group-size")
						.hasArg()
						.desc(
								"Number of events in out-of-order groups. 1 means no out-of-order events. Defaults to 1.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("first-event-id")
						.hasArg()
						.desc("First event id for the generator. Allows resuming from a specific position. Defaults to 1.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("print-script")
						.desc("Log the expanded SQL script before execution.")
						.build());
		options.addOption(
				Option.builder()
						.longOpt("wait-for-finish")
						.desc("Wait for the submitted job to finish before exiting.")
						.build());
		options.addOption(
				Option.builder("h").longOpt("help").desc("Show usage information.").build());
		return options;
	}

	private static void printHelp(Options options) {
		new HelpFormatter().printHelp("SqlQueryJob", options, true);
	}

	private static String joinStatements(List<String> statements) {
		StringBuilder builder = new StringBuilder();
		for (String statement : statements) {
			builder.append(statement).append('\n');
		}
		return builder.toString();
	}

	private static final class QueryOptions {

		private final String queryName;
		private final String sourceType;
		private final String bootstrapServers;
		private final long tps;
		private final long events;
		private final int personProportion;
		private final int auctionProportion;
		private final int bidProportion;
		private final boolean keepAlive;
		private final long stopAt;
		private final boolean maxEmitSpeed;
		private final long baseTimeMillis;
		private final int numInFlightAuctions;
		private final long occasionalDelayMinSec;
		private final long occasionalDelaySec;
		private final double probDelayedEvent;
		private final long outOfOrderGroupSize;
		private final long firstEventId;
		private final String jobName;
		private final String savepointPath;
		private final boolean printScript;
		private final boolean waitForFinish;

		private QueryOptions(
				String queryName,
				String sourceType,
				String bootstrapServers,
				long tps,
				long events,
				int personProportion,
				int auctionProportion,
				int bidProportion,
				boolean keepAlive,
				long stopAt,
				boolean maxEmitSpeed,
				long baseTimeMillis,
				int numInFlightAuctions,
				long occasionalDelayMinSec,
				long occasionalDelaySec,
				double probDelayedEvent,
				long outOfOrderGroupSize,
				long firstEventId,
				String jobName,
				String savepointPath,
				boolean printScript,
				boolean waitForFinish) {
			this.queryName = queryName;
			this.sourceType = sourceType;
			this.bootstrapServers = bootstrapServers;
			this.tps = tps;
			this.events = events;
			this.personProportion = personProportion;
			this.auctionProportion = auctionProportion;
			this.bidProportion = bidProportion;
			this.keepAlive = keepAlive;
			this.stopAt = stopAt;
			this.maxEmitSpeed = maxEmitSpeed;
			this.baseTimeMillis = baseTimeMillis;
			this.numInFlightAuctions = numInFlightAuctions;
			this.occasionalDelayMinSec = occasionalDelayMinSec;
			this.occasionalDelaySec = occasionalDelaySec;
			this.probDelayedEvent = probDelayedEvent;
			this.outOfOrderGroupSize = outOfOrderGroupSize;
			this.firstEventId = firstEventId;
			this.jobName = jobName;
			this.savepointPath = savepointPath;
			this.printScript = printScript;
			this.waitForFinish = waitForFinish;
		}

		private static QueryOptions from(CommandLine commandLine) {
			String queryName = commandLine.getOptionValue("query");
			String sourceType = commandLine.getOptionValue("source", "datagen").toLowerCase();
			if (!"datagen".equals(sourceType) && !"kafka".equals(sourceType)) {
				throw new IllegalArgumentException("Unsupported source type: " + sourceType);
			}

			String bootstrapServers = commandLine.getOptionValue("bootstrap-servers");
			if ((usesKafkaSource(sourceType) || requiresKafkaSink(queryName))
					&& (bootstrapServers == null || bootstrapServers.trim().isEmpty())) {
				throw new IllegalArgumentException(
						"--bootstrap-servers is required when Kafka is used as a source or sink.");
			}

			return new QueryOptions(
					queryName,
					sourceType,
					bootstrapServers,
					Long.parseLong(commandLine.getOptionValue("tps", "10000")),
					Long.parseLong(commandLine.getOptionValue("events", "0")),
					Integer.parseInt(commandLine.getOptionValue("person-proportion", "1")),
					Integer.parseInt(commandLine.getOptionValue("auction-proportion", "3")),
					Integer.parseInt(commandLine.getOptionValue("bid-proportion", "46")),
					Boolean.parseBoolean(commandLine.getOptionValue("keep-alive", "false")),
					Long.parseLong(commandLine.getOptionValue("stop-at", "-1")),
					Boolean.parseBoolean(commandLine.getOptionValue("max-emit-speed", "true")),
					Long.parseLong(
							commandLine.getOptionValue(
									"base-time-millis",
									String.valueOf(System.currentTimeMillis()))),
					Integer.parseInt(
							commandLine.getOptionValue("num-in-flight-auctions", "100")),
					Long.parseLong(
							commandLine.getOptionValue("occasional-delay-min-sec", "60")),
					Long.parseLong(commandLine.getOptionValue("occasional-delay-sec", "240")),
					Double.parseDouble(commandLine.getOptionValue("prob-delayed-event", "0")),
						Long.parseLong(commandLine.getOptionValue("out-of-order-group-size", "1")),
						Long.parseLong(commandLine.getOptionValue("first-event-id", "1")),
						commandLine.getOptionValue("job-name"),
						commandLine.getOptionValue("savepoint"),
						commandLine.hasOption("print-script"),
						commandLine.hasOption("wait-for-finish"));
		}

		private static boolean requiresKafkaSink(String queryName) {
			return queryName.startsWith("insert_kafka");
		}

		private static boolean usesKafkaSource(String sourceType) {
			return "kafka".equals(sourceType);
		}

		private boolean usesKafkaSource() {
			return usesKafkaSource(sourceType);
		}

		private boolean isUniqueQuery() {
			return queryName.contains("_unique");
		}

		private boolean isKafkaPrepareQuery() {
			return queryName.startsWith("insert_kafka");
		}

		private boolean shouldReadUniqueKafkaTables() {
			return isUniqueQuery() && usesKafkaSource() && !isKafkaPrepareQuery();
		}

		private Map<String, String> toTemplateVariables() {
			Map<String, String> variables = new LinkedHashMap<>();
			variables.put("NEXMARK_TABLE", usesKafkaSource() ? "kafka" : "datagen");
			variables.put("PERSON_TABLE", shouldReadUniqueKafkaTables() ? "person_kafka" : "person_src");
			variables.put("AUCTION_TABLE", shouldReadUniqueKafkaTables() ? "auction_kafka" : "auction_src");
			variables.put("BID_TABLE", shouldReadUniqueKafkaTables() ? "bid_kafka" : "bid_src");
			variables.put(
					"BID_MODIFIED_TABLE",
					shouldReadUniqueKafkaTables() ? "bid_kafka" : "bid_modified_src");
			variables.put("BASE_TIME_MILLIS", String.valueOf(baseTimeMillis));
			variables.put("TPS", String.valueOf(tps));
			variables.put("EVENTS_NUM", String.valueOf(events));
			variables.put("PERSON_PROPORTION", String.valueOf(personProportion));
			variables.put("AUCTION_PROPORTION", String.valueOf(auctionProportion));
			variables.put("BID_PROPORTION", String.valueOf(bidProportion));
			variables.put("KEEP_ALIVE", String.valueOf(keepAlive));
			variables.put("STOP_AT", String.valueOf(stopAt));
			variables.put("MAX_EMIT_SPEED", String.valueOf(maxEmitSpeed));
			variables.put(
					"OCCASIONAL_DELAY_MIN_SEC", String.valueOf(occasionalDelayMinSec));
			variables.put("OCCASIONAL_DELAY_SEC", String.valueOf(occasionalDelaySec));
			variables.put("PROB_DELAYED_EVENT", String.valueOf(probDelayedEvent));
			variables.put("OUT_OF_ORDER_GROUP_SIZE", String.valueOf(outOfOrderGroupSize));
			variables.put("NUM_IN_FLIGHT_AUCTIONS", String.valueOf(numInFlightAuctions));
			variables.put("FIRST_EVENT_ID", String.valueOf(firstEventId));
			variables.put(
					"BOOTSTRAP_SERVERS",
					bootstrapServers == null ? "" : bootstrapServers.trim());
			return variables;
		}
	}
}
