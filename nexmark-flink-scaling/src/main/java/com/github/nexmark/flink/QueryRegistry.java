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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Central registry for query metadata that is not discoverable from the filesystem.
 *
 * <p>Today this only contains DataStream based queries, which require additional information
 * such as the fully qualified main class name when submitting through {@link QueryRunner}.
 */
public final class QueryRegistry {

	private static final Map<String, DataStreamQueryDescriptor> DATASTREAM_QUERIES = buildDataStreamQueries();

	private QueryRegistry() {
	}

	private static Map<String, DataStreamQueryDescriptor> buildDataStreamQueries() {
		Map<String, DataStreamQueryDescriptor> descriptors = new HashMap<>();
		descriptors.put(
			"q3_datastream",
			new DataStreamQueryDescriptor(
				"com.github.nexmark.flink.datastream.Query3DataStreamJob",
				"q3 datastream"));
		return Collections.unmodifiableMap(descriptors);
	}

	public static boolean isDataStreamQuery(String queryName) {
		return DATASTREAM_QUERIES.containsKey(queryName);
	}

	public static DataStreamQueryDescriptor getDataStreamQuery(String queryName) {
		DataStreamQueryDescriptor descriptor = DATASTREAM_QUERIES.get(queryName);
		if (descriptor == null) {
			throw new IllegalArgumentException("Unknown DataStream query: " + queryName);
		}
		return descriptor;
	}

	public static Set<String> getAllDataStreamQueries() {
		return DATASTREAM_QUERIES.keySet();
	}

	public static final class DataStreamQueryDescriptor {
		private final String mainClass;
		private final String defaultJobName;

		DataStreamQueryDescriptor(String mainClass, String defaultJobName) {
			this.mainClass = mainClass;
			this.defaultJobName = defaultJobName;
		}

		public String getMainClass() {
			return mainClass;
		}

		public String getDefaultJobName() {
			return defaultJobName;
		}
	}
}
