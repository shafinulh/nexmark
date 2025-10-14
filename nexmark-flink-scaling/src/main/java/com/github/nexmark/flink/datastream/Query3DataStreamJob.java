package com.github.nexmark.flink.datastream;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.source.NexmarkSource;
import com.github.nexmark.flink.source.NexmarkSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
// import org.apache.flink.api.java.utils.ParameterTool;
// import org.apache.flink.client.cli.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * DataStream implementation of Nexmark Query 3
 */
public class Query3DataStreamJob {

	private static final Set<String> TARGET_STATES = new HashSet<>(Arrays.asList("OR", "ID", "CA"));
	private static final long TARGET_CATEGORY = 10L;

	private static final int DEFAULT_SOURCE_PARALLELISM = 1;
	private static final int DEFAULT_FILTER_PARALLELISM = DEFAULT_SOURCE_PARALLELISM;
	private static final int DEFAULT_JOIN_PARALLELISM = 1;
	private static final String DEFAULT_JOB_NAME = "Nexmark Query3 DataStream";

	public static void main(String[] args) throws Exception {
		Map<String, String> params = parseArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.disableOperatorChaining();

		int sourceParallelism = params.containsKey("source-parallelism")
			? Integer.parseInt(params.get("source-parallelism"))
			: DEFAULT_SOURCE_PARALLELISM;
		int filterParallelism = params.containsKey("filter-parallelism")
			? Integer.parseInt(params.get("filter-parallelism"))
			: sourceParallelism;
		int joinParallelism = params.containsKey("join-parallelism")
			? Integer.parseInt(params.get("join-parallelism"))
			: DEFAULT_JOIN_PARALLELISM;

		NexmarkConfiguration configuration = configurationFromParameters(params, sourceParallelism);
		NexmarkSource nexmarkSource = NexmarkSourceFactory.forDataStream(configuration);

		// TODO: add watermark on dateTime field and minibatch
		/*
		 * +- MiniBatchAssigner(interval=[2000ms], mode=[ProcTime])(reuse_id=[1])
	     * 		+- WatermarkAssigner(rowtime=[dateTime], watermark=[(dateTime - 4000:INTERVAL SECOND)]) 
		 * 			+- Calc(select=[event_type, person, auction, bid, CASE((event_type = 0), person.dateTime, (event_type = 1), auction.dateTime, bid.dateTime) AS dateTime])
		*/
		DataStream<RowData> events = env
			.fromSource(nexmarkSource, WatermarkStrategy.noWatermarks(), "Nexmark Source")
			.setParallelism(sourceParallelism)
			.slotSharingGroup("src");

		/*
		 * +- Calc(select=[auction.id AS id, auction.seller AS seller], where=[((event_type = 1) AND (auction.category = 10))])
		*/
		DataStream<SimpleAuction> auctions = events
			.flatMap(new AuctionProjector())
			.name("Auction Select + Filter")
			.returns(TypeInformation.of(SimpleAuction.class))
			.setParallelism(filterParallelism)
			.slotSharingGroup("src");

		/*
		 * +- Calc(select=[person.id AS id, person.name AS name, person.city AS city, person.state AS state], where=[((event_type = 0) AND SEARCH(person.state, Sarg[_UTF-16LE'CA', _UTF-16LE'ID', _UTF-16LE'OR']))])                                      
		*/
		DataStream<SimplePerson> persons = events
			.flatMap(new PersonProjector())
			.name("Person Select + Filter")
			.returns(TypeInformation.of(SimplePerson.class))
			.setParallelism(filterParallelism)
			.slotSharingGroup("src");

		/*
		 * Exchange(distribution=[hash[seller]]) 
		 * Exchange(distribution=[hash[id]]) 
		*/
		KeyedStream<SimpleAuction, Long> keyedAuctions = auctions.keyBy(new AuctionSellerKeySelector());
		KeyedStream<SimplePerson, Long> keyedPersons = persons.keyBy(new PersonIdKeySelector());
		
		/*
		 * +- Join(joinType=[InnerJoin], where=[(seller = id0)], select=[id, seller, id0, name, city, state], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey], miniBatch=[true]) 
		*/
		DataStream<Tuple4<String, String, String, Long>> joined = keyedAuctions
			.connect(keyedPersons)
			.flatMap(new JoinPersonsWithAuctions())
			.name("Incremental Join")
			.setParallelism(joinParallelism)
			.slotSharingGroup("join");

		joined
			.sinkTo(new DiscardingSink<>())
			.setParallelism(joinParallelism)
			.slotSharingGroup("join");

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

	private static SimpleAuction convertAuction(RowData auctionRow) {
		SimpleAuction auction = new SimpleAuction();
		auction.id = auctionRow.getLong(0);
		auction.seller = auctionRow.getLong(7);
		auction.category = auctionRow.getLong(8);
		auction.dateTime = getTimestamp(auctionRow, 5);
		return auction;
	}

	private static SimplePerson convertPerson(RowData personRow) {
		SimplePerson person = new SimplePerson();
		person.id = personRow.getLong(0);
		person.name = getString(personRow, 1);
		person.city = getString(personRow, 4);
		person.state = getString(personRow, 5);
		person.dateTime = getTimestamp(personRow, 6);
		return person;
	}

	private static String getString(RowData row, int pos) {
		if (row.isNullAt(pos)) {
			return null;
		}
		StringData data = row.getString(pos);
		return data == null ? null : data.toString();
	}

	private static long getTimestamp(RowData row, int pos) {
		if (row.isNullAt(pos)) {
			return 0L;
		}
		TimestampData timestamp = row.getTimestamp(pos, 3);
		return timestamp == null ? 0L : timestamp.getMillisecond();
	}

	private static final class AuctionSellerKeySelector implements KeySelector<SimpleAuction, Long> {
		@Override
		public Long getKey(SimpleAuction auction) {
			return auction.seller;
		}
	}

	private static final class PersonIdKeySelector implements KeySelector<SimplePerson, Long> {
		@Override
		public Long getKey(SimplePerson person) {
			return person.id;
		}
	}

	private static final class JoinPersonsWithAuctions extends RichCoFlatMapFunction<SimpleAuction, SimplePerson, Tuple4<String, String, String, Long>> {

		private transient ValueState<SimplePerson> personState;
		private transient ListState<Long> pendingAuctionIds;

		@Override
		public void open(OpenContext openContext) throws Exception {
			ValueStateDescriptor<SimplePerson> personDescriptor =
				new ValueStateDescriptor<>("person-state", TypeInformation.of(SimplePerson.class));
			personState = getRuntimeContext().getState(personDescriptor);

			ListStateDescriptor<Long> auctionsDescriptor =
				new ListStateDescriptor<>("auction-state", Types.LONG);
			pendingAuctionIds = getRuntimeContext().getListState(auctionsDescriptor);
		}

		@Override
		public void flatMap1(SimpleAuction auction, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
			SimplePerson person = personState.value();
			if (person != null) {
				out.collect(Tuple4.of(person.name, person.city, person.state, auction.id));
			} else {
				pendingAuctionIds.add(auction.id);
			}
		}

		@Override
		public void flatMap2(SimplePerson person, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
			personState.update(person);
			Iterable<Long> pending = pendingAuctionIds.get();
			if (pending != null) {
				for (Long auctionId : pending) {
					out.collect(Tuple4.of(person.name, person.city, person.state, auctionId));
				}
			}
			pendingAuctionIds.clear();
		}
	}

	public static final class SimpleAuction {
		public long id;
		public long seller;
		public long category;
		public long dateTime;
	}

	public static final class SimplePerson {
		public long id;
		public String name;
		public String city;
		public String state;
		public long dateTime;
	}

	private static final class AuctionProjector implements FlatMapFunction<RowData, SimpleAuction> {
		@Override
		public void flatMap(RowData row, Collector<SimpleAuction> out) {
			if (row.getInt(0) == com.github.nexmark.flink.model.Event.Type.AUCTION.value) {
				SimpleAuction auction = convertAuction(row.getRow(2, 10));
				if (auction.category == TARGET_CATEGORY) {
					out.collect(auction);
				}
			}
		}
	}

	private static final class PersonProjector implements FlatMapFunction<RowData, SimplePerson> {
		@Override
		public void flatMap(RowData row, Collector<SimplePerson> out) {
			if (row.getInt(0) == com.github.nexmark.flink.model.Event.Type.PERSON.value) {
				SimplePerson person = convertPerson(row.getRow(1, 8));
				if (TARGET_STATES.contains(person.state)) {
					out.collect(person);
				}
			}
		}
	}
}
