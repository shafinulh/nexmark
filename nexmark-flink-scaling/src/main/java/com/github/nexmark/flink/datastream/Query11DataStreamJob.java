package com.github.nexmark.flink.datastream;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.source.NexmarkSource;
import com.github.nexmark.flink.source.NexmarkSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
// import org.apache.flink.streaming.api.windowing.time.Time;
// import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * DataStream implementation of Nexmark Query 11.
 */
public class Query11DataStreamJob {

	// private static final long DEFAULT_SESSION_GAP_MS = Time.seconds(10).toMilliseconds();
	private static final long DEFAULT_SESSION_GAP_MS = Duration.ofSeconds(10).toMillis();

	private static final int DEFAULT_SOURCE_PARALLELISM = 1;
	private static final int DEFAULT_PROJECT_PARALLELISM = DEFAULT_SOURCE_PARALLELISM;
	private static final int DEFAULT_SESSION_PARALLELISM = 1;
	private static final String DEFAULT_JOB_NAME = "Nexmark Query11 DataStream";

	public static void main(String[] args) throws Exception {
		Map<String, String> params = parseArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.disableOperatorChaining();

		int sourceParallelism = params.containsKey("source-parallelism")
			? Integer.parseInt(params.get("source-parallelism"))
			: DEFAULT_SOURCE_PARALLELISM;
		int projectParallelism = params.containsKey("project-parallelism")
			? Integer.parseInt(params.get("project-parallelism"))
			: DEFAULT_PROJECT_PARALLELISM;
		int sessionParallelism = params.containsKey("session-parallelism")
			? Integer.parseInt(params.get("session-parallelism"))
			: DEFAULT_SESSION_PARALLELISM;

		NexmarkConfiguration configuration = configurationFromParameters(params, sourceParallelism);
		NexmarkSource nexmarkSource = NexmarkSourceFactory.forDataStream(configuration);

		// Time sessionGap = Time.milliseconds(sessionGapMillis);
		Duration sessionGap = Duration.ofMillis(DEFAULT_SESSION_GAP_MS);

		// TODO: add watermark on dateTime field and minibatch alignment with SQL execution plan
		/*
		 * +- MiniBatchAssigner(interval=[2000ms], mode=[RowTime])
		 * 		+- WatermarkAssigner(rowtime=[dateTime], watermark=[(dateTime - 4000:INTERVAL SECOND)])
		 * 			+- Calc(select=[event_type, person, auction, bid, CASE((event_type = 0), person.dateTime, (event_type = 1), auction.dateTime, bid.dateTime) AS dateTime])
		 */
		DataStream<RowData> events = env
			.fromSource(nexmarkSource, WatermarkStrategy.noWatermarks(), "Nexmark Source")
			.setParallelism(sourceParallelism)
			.slotSharingGroup("src");

		/*
		 * +- Calc(select=[bid.bidder AS $f0, dateTime], where=[(event_type = 2)])
		 */
		DataStream<SimpleBid> bids = events
			.flatMap(new BidProjector())
			.name("Bid Select + Filter")
			.returns(TypeInformation.of(SimpleBid.class))
			.setParallelism(projectParallelism)
			.slotSharingGroup("src");

		DataStream<SimpleBid> timestampedBids = bids
			.assignTimestampsAndWatermarks(
				WatermarkStrategy
					.<SimpleBid>forMonotonousTimestamps()
					.withTimestampAssigner((event, ts) -> event.dateTime))
			.name("Assign Event Time")
			.setParallelism(projectParallelism)
			.slotSharingGroup("src");

		/*
		 * +- Exchange(distribution=[hash[$f0]])
		 */
		KeyedStream<SimpleBid, Long> keyedBids = timestampedBids.keyBy(SimpleBid::getBidder);

		/*
		 * +- GroupWindowAggregate(groupBy=[$f0], window=[SessionGroupWindow('w$, dateTime, 10000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[$f0, COUNT(*) AS bid_count, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime])
		 */
		DataStream<WindowedBidCount> sessionCounts = keyedBids
			.window(EventTimeSessionWindows.withGap(sessionGap))
			.aggregate(new CountBidsAggregate(), new EmitSessionWindow())
			.name("Session Window Aggregate")
			.setParallelism(sessionParallelism)
			.slotSharingGroup("session");

		/*
		 * +- Calc(select=[$f0 AS bidder, bid_count, w$start AS starttime, w$end AS endtime])
		 */
		DataStream<BidSessionOutput> projectedResults = sessionCounts
			.map(new ProjectSessionWindow())
			.name("Output Projection")
			.returns(TypeInformation.of(BidSessionOutput.class))
			.setParallelism(sessionParallelism)
			.slotSharingGroup("session");

		// +- Sink(table=[nexmark_q11], fields=[bidder, bid_count, starttime, endtime])
		projectedResults
			.sinkTo(new DiscardingSink<>())
			.setParallelism(sessionParallelism)
			.slotSharingGroup("session");

		String jobName = params.getOrDefault("job-name", DEFAULT_JOB_NAME);
		env.execute(jobName);
	}

	private static Map<String, String> parseArgs(String[] args) {
		Map<String, String> parsed = new HashMap<>();
		for (int i = 0; i < args.length - 1; i += 2) {
			String key = args[i];
			String value = args[i + 1];
			if (key.startsWith("--")) {
				parsed.put(key.substring(2), value);
			}
		}
		return parsed;
	}

	private static NexmarkConfiguration configurationFromParameters(Map<String, String> params, int sourceParallelism) {
		NexmarkConfiguration configuration = defaultConfiguration(sourceParallelism);

		long events = params.containsKey("events")
			? Long.parseLong(params.get("events"))
			: configuration.numEvents;
		configuration.numEvents = events;

		long tps = params.containsKey("tps")
			? Long.parseLong(params.get("tps"))
			: configuration.nextEventRate;
		int rate = (int) tps;
		configuration.firstEventRate = rate;
		configuration.nextEventRate = rate;

		configuration.personProportion = params.containsKey("person-proportion")
			? Integer.parseInt(params.get("person-proportion"))
			: configuration.personProportion;
		configuration.auctionProportion = params.containsKey("auction-proportion")
			? Integer.parseInt(params.get("auction-proportion"))
			: configuration.auctionProportion;
		configuration.bidProportion = params.containsKey("bid-proportion")
			? Integer.parseInt(params.get("bid-proportion"))
			: configuration.bidProportion;

		return configuration;
	}

	private static NexmarkConfiguration defaultConfiguration(int sourceParallelism) {
		NexmarkConfiguration configuration = new NexmarkConfiguration();
		configuration.numEventGenerators = sourceParallelism;
		configuration.firstEventRate = 10000000;
		configuration.nextEventRate = 10000000;
		configuration.numEvents = 100000000L;
		return configuration;
	}

	private static SimpleBid convertBid(RowData bidRow) {
		SimpleBid bid = new SimpleBid();
		bid.bidder = bidRow.getLong(1);
		bid.dateTime = getTimestamp(bidRow, 5);
		return bid;
	}

	private static long getTimestamp(RowData row, int pos) {
		if (row.isNullAt(pos)) {
			return 0L;
		}
		TimestampData timestamp = row.getTimestamp(pos, 3);
		return timestamp == null ? 0L : timestamp.getMillisecond();
	}

	private static final class BidProjector implements FlatMapFunction<RowData, SimpleBid> {
		@Override
		public void flatMap(RowData row, Collector<SimpleBid> out) {
			if (row.getInt(0) == com.github.nexmark.flink.model.Event.Type.BID.value) {
				SimpleBid bid = convertBid(row.getRow(3, 7));
				out.collect(bid);
			}
		}
	}

	private static final class CountBidsAggregate implements org.apache.flink.api.common.functions.AggregateFunction<SimpleBid, Long, Long> {
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(SimpleBid value, Long accumulator) {
			return accumulator + 1;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	private static final class EmitSessionWindow extends ProcessWindowFunction<Long, WindowedBidCount, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<Long> elements, Collector<WindowedBidCount> out) {
			Long count = elements.iterator().next();
			WindowedBidCount result = new WindowedBidCount();
			result.bidder = key;
			result.bidCount = count;
			result.windowStart = context.window().getStart();
			result.windowEnd = context.window().getEnd();
			out.collect(result);
		}
	}

	public static final class SimpleBid {
		public long bidder;
		public long dateTime;

		public long getBidder() {
			return bidder;
		}
	}

	private static final class ProjectSessionWindow implements org.apache.flink.api.common.functions.MapFunction<WindowedBidCount, BidSessionOutput> {
		@Override
		public BidSessionOutput map(WindowedBidCount value) {
			BidSessionOutput output = new BidSessionOutput();
			output.bidder = value.bidder;
			output.bidCount = value.bidCount;
			output.startTime = value.windowStart;
			output.endTime = value.windowEnd;
			return output;
		}
	}

	public static final class WindowedBidCount {
		public long bidder;
		public long bidCount;
		public long windowStart;
		public long windowEnd;
	}

	public static final class BidSessionOutput {
		public long bidder;
		public long bidCount;
		public long startTime;
		public long endTime;
	}
}
