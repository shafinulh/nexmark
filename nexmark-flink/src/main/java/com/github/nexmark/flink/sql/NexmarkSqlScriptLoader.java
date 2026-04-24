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

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Loads Nexmark SQL resources from the jar and expands template variables. */
final class NexmarkSqlScriptLoader {

	private static final List<String> QUERY_DIRECTORIES =
			Arrays.asList("queries", "queries-cep");
	private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

	List<String> loadStatements(String queryName, boolean kafkaSource, Map<String, String> variables)
			throws IOException {
		String queryDirectory = resolveQueryDirectory(queryName);
		List<String> resourceContents = new ArrayList<>();

		if (shouldLoadDatagenDdl(queryName, kafkaSource)) {
			resourceContents.add(
					render(
							loadFirstExistingResource(
									queryDirectory + "/" + getDatagenDdlFile(queryName),
									queryDirectory + "/ddl_gen_v2.sql",
									queryDirectory + "/ddl_gen.sql"),
							variables));
		}

		if (shouldLoadKafkaDdl(queryName, kafkaSource)) {
			resourceContents.add(
					render(
							loadFirstExistingResource(
									queryDirectory + "/" + getKafkaDdlFile(queryName),
									queryDirectory + "/ddl_kafka_sink.sql",
									queryDirectory + "/ddl_kafka.sql"),
							variables));
		}

		resourceContents.add(
				render(
						loadFirstExistingResource(
								queryDirectory + "/" + getViewsDdlFile(queryName),
								queryDirectory + "/ddl_views.sql"),
						variables));
		resourceContents.add(
				render(loadRequiredResource(queryDirectory + "/" + queryName + ".sql"), variables));

		StringBuilder script = new StringBuilder();
		for (String content : resourceContents) {
			script.append(content);
			if (!content.endsWith("\n")) {
				script.append('\n');
			}
		}
		return SqlScriptParser.splitStatements(script.toString());
	}

	private boolean shouldLoadDatagenDdl(String queryName, boolean kafkaSource) {
		return isKafkaPrepareQuery(queryName) || !kafkaSource;
	}

	private boolean shouldLoadKafkaDdl(String queryName, boolean kafkaSource) {
		return isKafkaPrepareQuery(queryName) || kafkaSource;
	}

	private String getDatagenDdlFile(String queryName) {
		return isUniqueQuery(queryName) ? "ddl_gen_unique_v2.sql" : "ddl_gen_v2.sql";
	}

	private String getKafkaDdlFile(String queryName) {
		return isUniqueQuery(queryName) ? "ddl_kafka_unique.sql" : "ddl_kafka.sql";
	}

	private String getViewsDdlFile(String queryName) {
		return isUniqueQuery(queryName) ? "ddl_views_unique.sql" : "ddl_views.sql";
	}

	private boolean isUniqueQuery(String queryName) {
		return queryName.contains("_unique");
	}

	private boolean isKafkaPrepareQuery(String queryName) {
		return queryName.startsWith("insert_kafka");
	}

	private String resolveQueryDirectory(String queryName) throws IOException {
		for (String queryDirectory : QUERY_DIRECTORIES) {
			if (resourceExists(queryDirectory + "/" + queryName + ".sql")) {
				return queryDirectory;
			}
		}
		throw new IOException("Unknown Nexmark SQL query: " + queryName);
	}

	private String loadFirstExistingResource(String... resourcePaths) throws IOException {
		for (String resourcePath : resourcePaths) {
			if (resourceExists(resourcePath)) {
				return loadRequiredResource(resourcePath);
			}
		}
		throw new IOException("Unable to locate any of the expected resources.");
	}

	private String loadRequiredResource(String resourcePath) throws IOException {
		InputStream inputStream =
				Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
		if (inputStream == null) {
			throw new IOException("Missing SQL resource: " + resourcePath);
		}
		try {
			return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
		} finally {
			inputStream.close();
		}
	}

	private boolean resourceExists(String resourcePath) {
		InputStream inputStream =
				Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
		if (inputStream == null) {
			return false;
		}
		try {
			inputStream.close();
		} catch (IOException e) {
			return false;
		}
		return true;
	}

	private String render(String template, Map<String, String> variables) {
		String rendered = template;
		for (Map.Entry<String, String> entry : variables.entrySet()) {
			rendered = rendered.replace("${" + entry.getKey() + "}", entry.getValue());
		}

		Matcher matcher = TEMPLATE_PATTERN.matcher(rendered);
		if (matcher.find()) {
			throw new IllegalArgumentException(
					"Unresolved template variable in SQL resource: " + matcher.group(1));
		}
		return rendered;
	}
}
