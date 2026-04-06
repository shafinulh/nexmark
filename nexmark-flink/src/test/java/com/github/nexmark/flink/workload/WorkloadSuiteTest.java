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

package com.github.nexmark.flink.workload;

import org.apache.flink.configuration.Configuration;

import com.github.nexmark.flink.FlinkNexmarkOptions;
import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.utils.NexmarkGlobalConfigurationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.github.nexmark.flink.Benchmark.CATEGORY_OA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class WorkloadSuiteTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private static final String CATEGORY_CEP = "cep";

	@Test
	public void testCustomizedConf() {
		Configuration conf = new Configuration();
		conf.setString("nexmark.workload.suite.8m.tps", "8000000");
		conf.setString("nexmark.workload.suite.8m.events.num", "80000000");
		conf.setString("nexmark.workload.suite.8m.queries", "q0,q1,q2,q10,q12,q13,q14");
		conf.setString("nexmark.workload.suite.8m.queries.cep", "q0");
		conf.setString("nexmark.workload.suite.2m-no-bid.tps", "2000000");
		conf.setString("nexmark.workload.suite.2m-no-bid.percentage", "bid:0, auction:9, person:1");
		conf.setString("nexmark.workload.suite.2m-no-bid.queries", "q3,q8");
		conf.setString("nexmark.workload.suite.2m-no-bid.queries.cep", "q1");
		conf.setString("nexmark.workload.suite.2m.tps", "2000000");
		conf.setString("nexmark.workload.suite.2m.queries", "q5,q15");
		conf.setString("nexmark.workload.suite.2m.queries.cep", "q2");
		conf.setString("nexmark.workload.suite.1m.tps", "1000000");
		conf.setString("nexmark.workload.suite.1m.queries", "q4,q7,q9,q11");
		conf.setString("nexmark.workload.suite.1m.queries.cep", "q3");

		Workload load8m = new Workload(8000000, 80000000, 1, 3, 46);
		Workload load2mNoBid = new Workload(2000000, 0, 1, 9, 0);
		Workload load2m = new Workload(2000000, 0, 1, 3, 46);
		Workload load1m = new Workload(1000000, 0, 1, 3, 46);

		Map<String, Workload> query2Workload = new HashMap<>();
		query2Workload.put("q0", load8m);
		query2Workload.put("q1", load8m);
		query2Workload.put("q2", load8m);
		query2Workload.put("q10", load8m);
		query2Workload.put("q12", load8m);
		query2Workload.put("q13", load8m);
		query2Workload.put("q14", load8m);

		query2Workload.put("q3", load2mNoBid);
		query2Workload.put("q8", load2mNoBid);

		query2Workload.put("q5", load2m);

		query2Workload.put("q4", load1m);
		query2Workload.put("q7", load1m);
		query2Workload.put("q9", load1m);
		query2Workload.put("q11", load1m);
		query2Workload.put("q15", load2m);

		WorkloadSuite expected = new WorkloadSuite(query2Workload);

		assertEquals(expected.toString(), WorkloadSuite.fromConf(conf, CATEGORY_OA).toString());

		query2Workload = new HashMap<>();

		query2Workload.put("q0", load8m);

		query2Workload.put("q1", load2mNoBid);

		query2Workload.put("q2", load2m);

		query2Workload.put("q3", load1m);

		expected = new WorkloadSuite(query2Workload);

		assertEquals(expected.toString(), WorkloadSuite.fromConf(conf, CATEGORY_CEP).toString());
	}

	@Test
	public void testDirectWorkloadKnobs() {
		Configuration conf = new Configuration();
		conf.setString("nexmark.workload.suite.custom.tps", "50000");
		conf.setString("nexmark.workload.suite.custom.events.num", "7000000");
		conf.setString("nexmark.workload.suite.custom.person.proportion", "2");
		conf.setString("nexmark.workload.suite.custom.auction.proportion", "25");
		conf.setString("nexmark.workload.suite.custom.bid.proportion", "73");
		conf.setString("nexmark.workload.suite.custom.num-in-flight-auctions", "2000");
		conf.setString("nexmark.workload.suite.custom.out-of-order-group-size", "1000");
		conf.setString("nexmark.workload.suite.custom.prob-delayed-event", "0.1");
		conf.setString("nexmark.workload.suite.custom.occasional-delay.min-sec", "60");
		conf.setString("nexmark.workload.suite.custom.occasional-delay.sec", "240");
		conf.setString("nexmark.workload.suite.custom.queries", "q20_unique");

		Workload workload = WorkloadSuite.fromConf(conf, CATEGORY_OA).getQueryWorkload("q20_unique");

		assertEquals(2, workload.getPersonProportion());
		assertEquals(25, workload.getAuctionProportion());
		assertEquals(73, workload.getBidProportion());
		assertEquals(2000, workload.getNumInFlightAuctions());
		assertEquals(1000L, workload.getOutOfOrderGroupSize());
		assertEquals(0.1, workload.getProbDelayedEvent(), 0.0);
	}

	@Test
	public void testDefaultConf() {
		URL confDir = NexmarkGlobalConfigurationTest.class.getClassLoader().getResource("conf");
		assert confDir != null;
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration(confDir.getPath());

		WorkloadSuite oaSuite = WorkloadSuite.fromConf(conf, CATEGORY_OA);
		WorkloadSuite cepSuite = WorkloadSuite.fromConf(conf, CATEGORY_CEP);

		Workload oaLoad = oaSuite.getQueryWorkload("q0");
		assertNotNull(oaLoad);
		assertEquals(50000L, oaLoad.getTps());
		assertEquals(12500000L, oaLoad.getEventsNum());
		assertEquals(1, oaLoad.getPersonProportion());
		assertEquals(3, oaLoad.getAuctionProportion());
		assertEquals(46, oaLoad.getBidProportion());
		assertEquals(100, oaLoad.getNumInFlightAuctions());
		assertEquals(1L, oaLoad.getOutOfOrderGroupSize());
		assertEquals(0.0, oaLoad.getProbDelayedEvent(), 0.0);

		assertEquals(oaLoad.toString(), oaSuite.getQueryWorkload("q20_unique").toString());
		assertEquals(oaLoad.toString(), oaSuite.getQueryWorkload("q20_unique_modified").toString());
		assertNull(oaSuite.getQueryWorkload("insert_kafka"));

		Workload cepLoad = cepSuite.getQueryWorkload("q0");
		assertNotNull(cepLoad);
		assertEquals(oaLoad.toString(), cepLoad.toString());
		assertNotNull(cepSuite.getQueryWorkload("q3"));
		assertNull(cepSuite.getQueryWorkload("q4"));
	}

	@Test
	public void testTPSValidation() {
		exception.expectMessage("You should configure 'nexmark.metric.monitor.duration'" +
				" in the TPS mode. Otherwise, the job will never end.");
		// TPS mode
		long eventsNum = 0L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(FlinkNexmarkOptions.METRIC_MONITOR_DURATION.defaultValue());
	}

	@Test
	public void testEventsNumValidation() {
		exception.expectMessage("The configuration of 'nexmark.metric.monitor.duration'" +
				" is not supported in the events number mode.");
		// EventsNum mode
		long eventsNum = 100L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(Duration.ofMillis(1000));
	}

	@Test
	public void testTPSMode() {
		// TPS mode
		long eventsNum = 0L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(Duration.ofMillis(1000));
	}

	@Test
	public void testEventsNumMode() {
		// EventsNum mode
		long eventsNum = 100L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(FlinkNexmarkOptions.METRIC_MONITOR_DURATION.defaultValue());
	}
}
