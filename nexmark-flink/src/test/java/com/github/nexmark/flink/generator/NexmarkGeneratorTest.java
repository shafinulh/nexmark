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

package com.github.nexmark.flink.generator;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.model.Event.Type;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NexmarkGeneratorTest {

	private static final long TEST_BASE_TIME = 1_000_000L;

	@Test
	public void reordersEventsWhenDelayEnabledForAuctionsAndBids() {
		NexmarkGenerator generator = createGenerator(60000, 1.0, 10, 10);
		long maxSeenId = Long.MIN_VALUE;
		boolean sawInversion = false;
		while (generator.hasNext()) {
			long eventId = generator.next().event.getEventId();
			if (eventId < maxSeenId) {
				sawInversion = true;
				break;
			}
			maxSeenId = Math.max(maxSeenId, eventId);
		}
		assertTrue("Expected delayed arrival to reorder emission", sawInversion);
	}

	@Test
	public void appliesDelayOnlyToAuctionAndBidWithinConfiguredRange() {
		NexmarkGenerator generator = createGenerator(90000, 1.0, 2, 5);
		while (generator.hasNext()) {
			NexmarkGenerator.NextEvent nextEvent = generator.next();
			long nominalWallclock =
				generator.getWallclockBaseTime() + (nextEvent.eventTimestamp - TEST_BASE_TIME);
			long appliedDelayMs = nextEvent.wallclockTimestamp - nominalWallclock;

			if (nextEvent.event.type == Type.PERSON) {
				assertEquals("Person events should not be delayed", 0L, appliedDelayMs);
			} else {
				assertTrue("Auction/Bid delay should be >= min", appliedDelayMs >= 2_000L);
				assertTrue("Auction/Bid delay should be <= max", appliedDelayMs <= 5_000L);
			}
		}
	}

	@Test
	public void preservesOrderWhenDelayProbabilityIsZero() {
		NexmarkGenerator generator = createGenerator(1000000, 0.0, 2, 5);
		long previousEventId = Long.MIN_VALUE;
		while (generator.hasNext()) {
			long currentEventId = generator.next().event.getEventId();
			assertTrue(
				"Event IDs should remain monotonic when delay probability is zero",
				currentEventId > previousEventId);
			previousEventId = currentEventId;
		}
	}

	private NexmarkGenerator createGenerator(
		long numEvents,
		double probDelayedEvent,
		long occasionalDelayMinSec,
		long occasionalDelayMaxSec) {
		NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
		nexmarkConfiguration.personProportion = 1;
		nexmarkConfiguration.auctionProportion = 1;
		nexmarkConfiguration.bidProportion = 1;
		nexmarkConfiguration.firstEventRate = 1;
		nexmarkConfiguration.numEventGenerators = 1;
		nexmarkConfiguration.outOfOrderGroupSize = 1;
		nexmarkConfiguration.probDelayedEvent = probDelayedEvent;
		nexmarkConfiguration.occasionalDelayMinSec = occasionalDelayMinSec;
		nexmarkConfiguration.occasionalDelaySec = occasionalDelayMaxSec;
		GeneratorConfig generatorConfig = new GeneratorConfig(
			nexmarkConfiguration,
			TEST_BASE_TIME,
			1,
			numEvents,
			-1L,
			1);
		return new NexmarkGenerator(generatorConfig);
	}
}
