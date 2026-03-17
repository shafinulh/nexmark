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

package com.github.nexmark.flink.source;

import com.github.nexmark.flink.generator.GeneratorConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

/**
 * Table source for Nexmark.
 */
public class NexmarkTableSource implements ScanTableSource, SupportsWatermarkPushDown {

	public static final Schema NEXMARK_SCHEMA = Schema.newBuilder()
		.column("event_type", INT())
		.column("person", ROW(
			FIELD("id", BIGINT()),
			FIELD("name", STRING()),
			FIELD("emailAddress", STRING()),
			FIELD("creditCard", STRING()),
			FIELD("city", STRING()),
			FIELD("state", STRING()),
			FIELD("dateTime", TIMESTAMP(3)),
			FIELD("extra", STRING())))
		.column("auction", ROW(
			FIELD("id", BIGINT()),
			FIELD("itemName", STRING()),
			FIELD("description", STRING()),
			FIELD("initialBid", BIGINT()),
			FIELD("reserve", BIGINT()),
			FIELD("dateTime", TIMESTAMP(3)),
			FIELD("expires", TIMESTAMP(3)),
			FIELD("seller", BIGINT()),
			FIELD("category", BIGINT()),
			FIELD("extra", STRING())))
		.column("bid", ROW(
			FIELD("auction", BIGINT()),
			FIELD("bidder", BIGINT()),
			FIELD("price", BIGINT()),
			FIELD("channel", STRING()),
			FIELD("url", STRING()),
			FIELD("dateTime", TIMESTAMP(3)),
			FIELD("extra", STRING())))
		.build();

	public static final ResolvedSchema RESOLVED_SCHEMA = ResolvedSchema.physical(
			NEXMARK_SCHEMA.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList()),
			NEXMARK_SCHEMA.getColumns().stream()
					.map(unresolvedColumn ->
							(DataType) ((Schema.UnresolvedPhysicalColumn) unresolvedColumn).getDataType())
					.collect(Collectors.toList()));

	public static final Schema PERSON_SCHEMA = Schema.newBuilder()
			.column("id", BIGINT())
			.column("name", STRING())
			.column("emailAddress", STRING())
			.column("creditCard", STRING())
			.column("city", STRING())
			.column("state", STRING())
			.column("dateTime", TIMESTAMP(3))
			.column("extra", STRING())
			.build();

	public static final ResolvedSchema RESOLVED_PERSON_SCHEMA = ResolvedSchema.physical(
			PERSON_SCHEMA.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList()),
			PERSON_SCHEMA.getColumns().stream()
					.map(unresolvedColumn ->
							(DataType) ((Schema.UnresolvedPhysicalColumn) unresolvedColumn).getDataType())
					.collect(Collectors.toList()));

	public static final Schema AUCTION_SCHEMA = Schema.newBuilder()
			.column("id", BIGINT())
			.column("itemName", STRING())
			.column("description", STRING())
			.column("initialBid", BIGINT())
			.column("reserve", BIGINT())
			.column("dateTime", TIMESTAMP(3))
			.column("expires", TIMESTAMP(3))
			.column("seller", BIGINT())
			.column("category", BIGINT())
			.column("extra", STRING())
			.build();

	public static final ResolvedSchema RESOLVED_AUCTION_SCHEMA = ResolvedSchema.physical(
			AUCTION_SCHEMA.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList()),
			AUCTION_SCHEMA.getColumns().stream()
					.map(unresolvedColumn ->
							(DataType) ((Schema.UnresolvedPhysicalColumn) unresolvedColumn).getDataType())
					.collect(Collectors.toList()));

	public static final Schema BID_SCHEMA = Schema.newBuilder()
			.column("id", BIGINT())
			.column("auction", BIGINT())
			.column("bidder", BIGINT())
			.column("price", BIGINT())
			.column("channel", STRING())
			.column("url", STRING())
			.column("dateTime", TIMESTAMP(3))
			.column("extra", STRING())
			.build();

	public static final ResolvedSchema RESOLVED_BID_SCHEMA = ResolvedSchema.physical(
			BID_SCHEMA.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList()),
			BID_SCHEMA.getColumns().stream()
					.map(unresolvedColumn ->
							(DataType) ((Schema.UnresolvedPhysicalColumn) unresolvedColumn).getDataType())
					.collect(Collectors.toList()));


	private final GeneratorConfig config;
	private final ResolvedSchema schema;
	private final NexmarkEventType eventType;
	private WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.noWatermarks();

	public NexmarkTableSource(GeneratorConfig config, ResolvedSchema schema, NexmarkEventType eventType) {
		this.config = config;
		this.schema = schema;
		this.eventType = eventType;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
		TypeInformation<RowData> outputType = scanContext
			.createTypeInformation(schema.toPhysicalRowDataType());
		return new DataStreamScanProvider() {
			@Override
			public DataStream<RowData> produceDataStream(
					ProviderContext providerContext,
					StreamExecutionEnvironment execEnv) {
				return execEnv.fromSource(
						new NexmarkSource(config, outputType, eventType),
						watermarkStrategy,
						"NexmarkSource");
			}

			@Override
			public boolean isBounded() {
				return false;
			}
		};
	}

	@Override
	public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
		this.watermarkStrategy = watermarkStrategy;
	}

	@Override
	public DynamicTableSource copy() {
		NexmarkTableSource copied = new NexmarkTableSource(config, schema, eventType);
		copied.watermarkStrategy = watermarkStrategy;
		return copied;
	}

	@Override
	public String asSummaryString() {
		return "Nexmark Source";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		NexmarkTableSource that = (NexmarkTableSource) o;
		return Objects.equals(config, that.config)
				&& Objects.equals(schema, that.schema)
				&& eventType == that.eventType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(config, schema, eventType);
	}

	@Override
	public String toString() {
		return "NexmarkTableSource{" +
			"config=" + config +
			", schema=" + schema +
			", eventType=" + eventType +
			'}';
	}
}
