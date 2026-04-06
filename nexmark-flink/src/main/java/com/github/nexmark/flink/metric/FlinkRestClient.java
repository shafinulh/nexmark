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

package com.github.nexmark.flink.metric;

import com.github.nexmark.flink.metric.tps.TpsMetric;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A HTTP client to request TPS metric to JobMaster REST API.
 */
public class FlinkRestClient {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);

	private static int CONNECT_TIMEOUT = 5000;
	private static int SOCKET_TIMEOUT = 60000;
	private static int CONNECTION_REQUEST_TIMEOUT = 10000;
	private static int MAX_IDLE_TIME = 60000;
	private static int MAX_CONN_TOTAL = 60;
	private static int MAX_CONN_PER_ROUTE = 30;

	private final String jmEndpoint;
	private final CloseableHttpClient httpClient;
	private final Map<String, String> jobIds;
	private volatile String lastJobId;

	public FlinkRestClient(String jmAddress, int jmPort) {
		this.jmEndpoint = jmAddress + ":" + jmPort;

		RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(SOCKET_TIMEOUT)
			.setConnectTimeout(CONNECT_TIMEOUT)
			.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
			.build();
		PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
		httpClientConnectionManager.setValidateAfterInactivity(MAX_IDLE_TIME);
		httpClientConnectionManager.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
		httpClientConnectionManager.setMaxTotal(MAX_CONN_TOTAL);

		this.httpClient = HttpClientBuilder.create()
			.setConnectionManager(httpClientConnectionManager)
			.setDefaultRequestConfig(requestConfig)
			.build();

		this.jobIds = new ConcurrentHashMap<>(50);
		this.lastJobId = "";
	}

	public synchronized void updateAllJobStatus() {
		String url = String.format("http://%s/jobs", jmEndpoint);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			JsonNode jobs = jsonNode.get("jobs");
			Map<String, String> latestStatuses = new ConcurrentHashMap<>(50);
			String latestJobId = "";
			for (JsonNode job : jobs) {
				String id = job.get("id").asText();
				latestStatuses.put(id, job.get("status").asText());
				if (latestJobId.isEmpty()) {
					latestJobId = id;
				}
			}
			jobIds.clear();
			jobIds.putAll(latestStatuses);
			lastJobId = latestJobId;
		} catch (JsonProcessingException e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public void cancelJob(String jobId) {
		LOG.info("Stopping Job: {}", jobId);
		String url = String.format("http://%s/jobs/%s?mode=cancel", jmEndpoint, jobId);
		patch(url);
	}

	public String triggerCheckpoint(String jobId) {
		String url = String.format("http://%s/jobs/%s/checkpoints", jmEndpoint, jobId);
		String data = "{\"checkpointType\":\"CONFIGURED\"}";
		String response = post(url, data);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			return jsonNode.get("request-id").asText();
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public String stopWithSavepoint(String jobId) {
		String url = String.format("http://%s/jobs/%s/stop", jmEndpoint, jobId);
		String data = "{\"formatType\":\"NATIVE\", \"drain\":\"true\"}";
		String response = post(url, data);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			return jsonNode.get("request-id").asText();
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public String getCurrentJobId() {
		updateAllJobStatus();
		return lastJobId;
	}

	public synchronized boolean isJobRunning(String jobId, long readCount) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			String state = jsonNode.get("state").asText();
			if (!state.equalsIgnoreCase("RUNNING")) {
				return false;
			}
			JsonNode vertices = jsonNode.get("vertices");
			if (vertices.isEmpty()) {
				return false;
			}
			return vertices.get(0).get("metrics").get("read-records").asLong() < readCount;
		} catch (JsonProcessingException e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	/**
	 * Returns the total write-records (records sent/emitted) summed across all source vertices
	 * for a running job. Returns -1 if the job is not in RUNNING state or has no vertices.
	 * Source vertices are identified by names starting with "Source:".
	 *
	 * Note: source vertices have read-records=0 (no upstream), so we use write-records.
	 */
	public synchronized long getTotalSourceReadRecords(String jobId) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			String state = jsonNode.get("state").asText();
			if (!state.equalsIgnoreCase("RUNNING")) {
				return -1L;
			}
			JsonNode vertices = jsonNode.get("vertices");
			if (vertices.isEmpty()) {
				return -1L;
			}
			long totalRecords = 0;
			for (JsonNode vertex : vertices) {
				String name = vertex.get("name").asText();
				if (name.startsWith("Source:")) {
					totalRecords += vertex.get("metrics").get("write-records").asLong();
				}
			}
			return totalRecords;
		} catch (JsonProcessingException e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public synchronized boolean isJobAndAllTasksRunning(String jobId) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			String state = jsonNode.get("state").asText();
			if (!state.equalsIgnoreCase("RUNNING")) {
				return false;
			}
			JsonNode vertices = jsonNode.get("vertices");
			if (vertices.isEmpty()) {
				return false;
			}
			for (JsonNode vertex : vertices) {
				String status = vertex.get("status").asText().toUpperCase();
				if (status.equals("CANCELING") || status.equals("FAILED") || status.equals("CANCELED")) {
					throw new RuntimeException("There is one task failed, canceling or canceled.");
				} else if (!status.equals("RUNNING") && !status.equals("FINISHED")) {
					return false;
				}
			}
			return true;
		} catch (JsonProcessingException e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public boolean isJobRunning(String jobId) {
		updateAllJobStatus();
		String status = isNullOrEmpty(jobId) ? null : jobIds.get(jobId);
		return status != null && status.equalsIgnoreCase("RUNNING");
	}

	public boolean isJobCanceledOrFinished(String jobId) {
		updateAllJobStatus();
		if (isNullOrEmpty(jobId)) {
			return true;
		}
		String status = jobIds.get(jobId);
		if (status == null) {
			// /jobs no longer reports this job, which is terminal for our polling loop.
			return true;
		}
		return status.equalsIgnoreCase("CANCELED")
			|| status.equalsIgnoreCase("FINISHED")
			|| status.equalsIgnoreCase("FAILED");
	}

	private static boolean isNullOrEmpty(String string) {
		return string == null || string.length() == 0;
	}

	public String getSourceVertexId(String jobId) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			JsonNode vertices = jsonNode.get("vertices");
			JsonNode sourceVertex = vertices.get(0);
			checkArgument(
				sourceVertex.get("name").asText().startsWith("Source:"),
				"The first vertex is not a source.");
			return sourceVertex.get("id").asText();
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public String getTpsMetricName(String jobId, String vertexId) {
		String url = String.format("http://%s/jobs/%s/vertices/%s/subtasks/metrics", jmEndpoint, jobId, vertexId);
		String response = executeAsString(url);
		try {
			ArrayNode arrayNode = (ArrayNode) NexmarkUtils.MAPPER.readTree(response);
			for (JsonNode node : arrayNode) {
				String metricName = node.get("id").asText();
				if (metricName.startsWith("Source_") && metricName.endsWith(".numRecordsOutPerSecond")) {
					return metricName;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
		throw new RuntimeException("Can't find TPS metric name from the response:\n" + response);
	}

	public synchronized TpsMetric getTpsMetric(String jobId, String vertexId, String tpsMetricName) {
		String url = String.format(
			"http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s",
			jmEndpoint,
			jobId,
			vertexId,
			tpsMetricName);
		String response = executeAsString(url);
		return TpsMetric.fromJson(response);
	}

	public Savepoint.Status checkCheckpointFinished(String jobId, String triggerId) {
		String url = String.format("http://%s/jobs/%s/checkpoints/%s", jmEndpoint, jobId, triggerId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			String status = jsonNode.get("status").get("id").asText();
			return Savepoint.Status.valueOf(status);
		} catch (Throwable e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public Savepoint.Status checkSavepointFinished(String jobId, String triggerId) {
		String url = String.format("http://%s/jobs/%s/savepoints/%s", jmEndpoint, jobId, triggerId);
		try {
			String response = executeAsString(url);
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			String status = jsonNode.get("status").get("id").asText();
			return Savepoint.Status.valueOf(status);
		} catch (RuntimeException e) {
			if (e.getMessage() != null && e.getMessage().contains("status code is 404")) {
				return Savepoint.Status.IN_PROGRESS;
			}
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException("Failed to check savepoint status for job " + jobId, e);
		}
	}

	public String getSavepointLocation(String jobId, String triggerId) {
		String url = String.format("http://%s/jobs/%s/savepoints/%s", jmEndpoint, jobId, triggerId);
		try {
			String response = executeAsString(url);
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			return parseSavepointLocation(jsonNode);
		} catch (RuntimeException e) {
			if (e.getMessage() != null && e.getMessage().contains("status code is 404")) {
				return null;
			}
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException("Failed to parse savepoint location for job " + jobId, e);
		}
	}

	private String parseSavepointLocation(JsonNode jsonNode) {
		if (jsonNode == null || jsonNode.isNull()) {
			return null;
		}
		JsonNode operation = jsonNode.get("operation");
		if (operation != null && operation.has("location")) {
			return operation.get("location").asText();
		}
		if (jsonNode.has("location")) {
			return jsonNode.get("location").asText();
		}
		JsonNode savepoint = jsonNode.get("savepoint");
		if (savepoint != null && savepoint.has("location")) {
			return savepoint.get("location").asText();
		}
		return null;
	}

	public Savepoint getJobLastCheckpoint(String jobId) {
		String url = String.format("http://%s/jobs/%s/checkpoints", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			return new Savepoint(
					Savepoint.Status.valueOf(jsonNode.get("latest").get("completed").get("status").asText()),
					jsonNode.get("latest").get("completed").get("external_path").asText());
		} catch (Throwable e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	private void patch(String url) {
		HttpPatch httpPatch = new HttpPatch();
		httpPatch.setURI(URI.create(url));
		HttpResponse response;
		try {
			httpPatch.setHeader("Connection", "close");
			response = httpClient.execute(httpPatch);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_ACCEPTED) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
		} catch (Exception e) {
			httpPatch.abort();
			throw new RuntimeException(e);
		}
	}

	private String post(String url, String data) {
		HttpPost httpPost = new HttpPost();
		httpPost.setURI(URI.create(url));
		HttpResponse response;
		try {
			httpPost.setHeader("Connection", "close");
			httpPost.setEntity(new StringEntity(data));
			response = httpClient.execute(httpPost);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_ACCEPTED) {
				String msg = String.format("http execute failed, status code is %d, response: %s", httpCode, EntityUtils.toString(response.getEntity()));
				throw new RuntimeException(msg);
			} else {
				return EntityUtils.toString(response.getEntity());
			}
		} catch (Exception e) {
			httpPost.abort();
			throw new RuntimeException(e);
		}
	}

	private String executeAsString(String url) {
		HttpGet httpGet = new HttpGet();
		httpGet.setURI(URI.create(url));
		try {
			HttpEntity entity = execute(httpGet).getEntity();
			if (entity != null) {
				return EntityUtils.toString(entity, Consts.UTF_8);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to request URL " + url, e);
		}
		throw new RuntimeException(String.format("Response of URL %s is null.", url));
	}

	private HttpResponse execute(HttpRequestBase httpRequestBase) throws Exception {
		HttpResponse response;
		try {
			httpRequestBase.setHeader("Connection", "close");
			response = httpClient.execute(httpRequestBase);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_OK) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
			return response;
		} catch (Exception e) {
			httpRequestBase.abort();
			throw e;
		}
	}

	public synchronized void close() {
		try {
			if (httpClient != null) {
				httpClient.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
