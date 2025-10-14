package com.github.nexmark.flink.source;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

/**
 * Helper to expose {@link NexmarkSource} for DataStream jobs.
 */
public final class NexmarkSourceFactory {

	private NexmarkSourceFactory() {
	}

	public static NexmarkSource forDataStream(NexmarkConfiguration configuration) {
		GeneratorConfig generatorConfig = new GeneratorConfig(
			configuration,
			System.currentTimeMillis(),
			1L,
			configuration.numEvents,
			1L);
		TypeInformation<RowData> typeInformation = TypeInformation.of(RowData.class);
		return new NexmarkSource(generatorConfig, typeInformation);
	}
}
