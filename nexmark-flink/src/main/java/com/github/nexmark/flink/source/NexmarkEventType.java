/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to You under
 * the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.nexmark.flink.source;

import com.github.nexmark.flink.model.Event;

import java.util.Locale;

/** Event-type filter for Nexmark source tables. */
public enum NexmarkEventType {
	ALL,
	PERSON,
	AUCTION,
	BID;

	public boolean matches(Event.Type type) {
		return this == ALL || name().equals(type.name());
	}

	public static NexmarkEventType fromOptionValue(String value) {
		return value == null ? ALL : valueOf(value.trim().toUpperCase(Locale.ROOT));
	}
}
