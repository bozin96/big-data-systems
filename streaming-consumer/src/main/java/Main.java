import models.StatisticRecord;
import models.TaxiRecord;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import java.util.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

public class Main {
    private static final String TaxiTopic = "taxi-data";
	private static final String Keyspace = "taxi_data";
	private static final String StatisticsTable = "statistics";
	private static final String TaxiRecordsTable = "taxi_records";

	public static void main(String[] args) throws Exception {

		System.out.println("STREAMING-CONSUMER IS STARTED");
		
		
		String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
		if (initialSleepTime != null && !initialSleepTime.equals("")) {
			int sleep = Integer.parseInt(initialSleepTime);
			System.out.println("INFO: Sleeping on start " + sleep + "sec");
			Thread.sleep(sleep * 1000);
		}

		String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
		if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
			throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
		}
		String kafkaUrl = System.getenv("KAFKA_URL");
		if (kafkaUrl == null || kafkaUrl.equals("")) {
			throw new IllegalStateException("KAFKA_URL environment variable must be set");
		}

		String dataReceivingTimeInSeconds = System.getenv("DATA_RECEIVING_TIME_IN_SECONDS");
		if (dataReceivingTimeInSeconds == null || dataReceivingTimeInSeconds.equals("")) {
			throw new IllegalStateException("DATA_RECEIVING_TIME_IN_SECONDS environment variable must be set");
		}
		int dataReceivingSleep = Integer.parseInt(dataReceivingTimeInSeconds);

		String cassandraUrl = System.getenv("CASSANDRA_URL");
		if (cassandraUrl == null || cassandraUrl.equals("")) {
			throw new IllegalStateException("CASSANDRA_URL environment variable must be set");
		}
		
		String cassandraPortStringValue = System.getenv("CASSANDRA_PORT");
		if (cassandraPortStringValue == null || cassandraPortStringValue.equals("")) {
			throw new IllegalStateException("CASSANDRA_PORT environment variable must be set");
		}
		Integer cassandraPort = Integer.parseInt(cassandraPortStringValue);

		// Creates Keyspace, Statistic and Taxi-Records tables if they don't exist.
		
		prepareCassandraKeyspace(cassandraUrl, cassandraPort);

		SparkConf conf = new SparkConf().setAppName("BigData-2").setMaster(sparkMasterUrl);
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(dataReceivingSleep * 1000));

		Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
		Collection<String> topics = Collections.singletonList(TaxiTopic);

		// Creates Kafka Direct Stream.
		
		JavaInputDStream<ConsumerRecord<Object, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

		
		JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);
		JavaDStream<EventData> eventData = receivedData.map(EventData::CreateEventData);

		// Prints received and serialized event data.
		
		eventData.print();
		
		Double minLongitude = -75.971770000000003;
		Double maxLongitude = -71.995770000000003;
		Double minLatitude = 38.706828000000003;
		Double maxLatitude = 42.756998000000003;
		
		
		// Filters trips with one passenger at a given location.
		
		JavaDStream<EventData> filteredData = eventData.filter(ed -> ed != null &&
				ed.getPickupLongitute() >=  minLongitude && ed.getPickupLongitute() <= maxLongitude &&
				ed.getDropoffLongitude() >=  minLongitude && ed.getDropoffLongitude() <=  maxLongitude && 
				ed.getPickupLatitude() >=  minLatitude && ed.getPickupLatitude() <= maxLatitude &&
				ed.getDropoffLatitude() >=  minLatitude && ed.getDropoffLatitude() <= maxLatitude &&
				ed.getPassengerCount().equals("1"));

		filteredData.foreachRDD(rdd -> {

			Long count = rdd.count();
			if (count <= 0) {
				System.out.println("INFO: There is no RDD over the filtered data, skipping.");
				return;
			}

			// Finds min, max and avg tip amount in current rdd batch.
			
			EventData minTipAmount = rdd.min(new TipAmountComparator());
			EventData maxTipAmount = rdd.max(new TipAmountComparator());

			EventData minPickupDate = rdd.min(new DateComparator());
			EventData maxPickupDate = rdd.max(new DateComparator());
			
            JavaRDD<Float> tipAmounts = rdd.map((tr) -> tr.getTipAmount());
            
            Float tipAmountSum = tipAmounts.reduce((tipAmount, accumulator) -> {
                return tipAmount + accumulator;
            });
            
            Float averageTipAmount = tipAmountSum / count;

			// Finds most used payment types in current rdd batch.
			
			JavaPairRDD<String, Integer> topPaymentTypes = rdd.mapToPair(r -> new Tuple2<>(r.getPaymentType(),1)).reduceByKey(Integer::sum);	
			Integer mostPaymentTypeCount = topPaymentTypes.map(t -> t._2).max(Comparator.naturalOrder());
			
			List<String> topPaymentTypesNames = topPaymentTypes.filter(t -> t._2.equals(mostPaymentTypeCount)).map(t -> t._1).collect();
		    String mostPaymentTypeNames = String.join(",", topPaymentTypesNames);
			
			// Finds most used rate codes in current rdd batch.
		    
			JavaPairRDD<String, Integer> topRateCodes = rdd.mapToPair(r -> new Tuple2<>(r.getRateCode(),1)).reduceByKey(Integer::sum);	
			Integer mostRateCodesCount = topRateCodes.map(t -> t._2).max(Comparator.naturalOrder());
			
			List<String> topRateCodeNames = topRateCodes.filter(t -> t._2.equals(mostRateCodesCount)).map(t -> t._1).collect();
		    String mostRateCodeNames = String.join(",", topRateCodeNames);
		    
		    // Creates and stores statistic data in Cassandra Db.
            
			StatisticRecord statisticData = new StatisticRecord(minPickupDate.getPickupDateTime(), maxPickupDate.getPickupDateTime(), 
					minTipAmount.getTipAmount(), maxTipAmount.getTipAmount(),averageTipAmount, count,
					mostPaymentTypeCount, mostPaymentTypeNames, mostRateCodesCount, mostRateCodeNames);

			//System.out.println("Statistic Data");
			//System.out.println(statisticData.toString());
			saveStatisticRecord(statisticData, cassandraUrl, cassandraPort);
			System.out.println("INFO: Statistics are stored in database.");
		    
		    
		    // Stores some record attributes in Cassandra Db for testing purpose.
		    
			rdd.foreach(c -> {
				
				TaxiRecord taxiRecord = new TaxiRecord(c.getPickupDateTime(), c.getDropoffDateTime(), 
						c.getFareAmount(), c.getPickupLatitude(), 
						c.getPickupLongitute(), c.getMtaTax(), 
						c.getRateCode(), c.getSurcharge());
				saveTaxiRecord(taxiRecord, cassandraUrl, cassandraPort);
				System.out.println("INFO: Taxi records are stored in database.");
			});
		      
			// Reading data from Cassandra Db.
			
			// readRecords(StatisticsTable, 10, cassandraUrl, cassandraPort);
			// readRecords(TaxiRecordsTable, 10, cassandraUrl, cassandraPort);
		});

		streamingContext.start();
		streamingContext.awaitTermination();
	}

	/**
	 * Creates HashMap with kafka consumer configuration parameters.
	 * @param brokers kafka's broker addresses
	 * @return kafka consumer configuration parameters
	 */
	public static Map<String, Object> getKafkaParams(String brokers) {
		
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		return kafkaParams;
	}

	/**
	 * Creates the necessary keyspace and tables if they do not exist.
	 * @param addr cassandra address
	 * @param port cassandra port
	 */
	public static void prepareCassandraKeyspace(String addr, Integer port) {
		
		CassandraConnector conn = CassandraConnector.getInstance();
		conn.connect(addr, port);
		CqlSession session = conn.getSession();
		

		String createKeyspaceCQL = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};", Keyspace);

		//String dropTaxiRecordTable = String.format("DROP TABLE %s.%s;", Keyspace, TaxiRecordsTable);
		
		String createTaxiRecordTable = String.format(
				"CREATE TABLE IF NOT EXISTS %s.%s (" 
							+ " start_time timestamp PRIMARY KEY," 
							+ " end_time timestamp,"
							+ " fare_amount float," 
							+ " pickup_latitude double," 
							+ " pickup_longitude double,"
							+ " mta_tax float," 
							+ " rate_code text," 
							+ " surcharge float );",
				Keyspace, TaxiRecordsTable);
		
		//String dropStatisticTable = String.format("DROP TABLE %s.%s;", Keyspace, StatisticsTable);
		
		String createStatisticTable = String.format(
				"CREATE TABLE IF NOT EXISTS %s.%s ("
					+ " min_pickup_time timestamp PRIMARY KEY," 
					+ " max_pickup_time timestamp," 
					+ " min_tip_amount float,"
					+ " max_tip_amount float," 
					+ " average_tip_amount float," 
					+ " trip_count bigint,"
					+ " top_payment_type_count int,"
					+ " top_payment_type_name text,"
					+ " top_rate_code_count int,"
					+ " top_rate_code_name text );", 
				Keyspace, StatisticsTable);

		System.out.println("Preparing Cassandra Keyspace.");

		session.execute(createKeyspaceCQL);
		System.out.println("Keyspace created");
		
		//session.execute(dropTaxiRecordTable);
		//System.out.println(String.format("Table %s dropped.", TaxiRecordsTable));
		
		session.execute(createTaxiRecordTable);
		System.out.println(String.format("Table %s created.", StatisticsTable));

		//session.execute(dropStatisticTable);
		//System.out.println(String.format("Table %s dropped.", StatisticsTable));
		
		session.execute(createStatisticTable);
		System.out.println(String.format("Table %s created.", TaxiRecordsTable));

		session.close();
		conn.close();
	}

	/** 
	 * Saves Statistic Record in Cassandra Db.
	 * @param statisticRecord
	 * @param addr
	 * @param port
	 */
	public static void saveStatisticRecord(StatisticRecord statisticRecord, String addr, Integer port) {
		
		CassandraConnector conn = CassandraConnector.getInstance();
		conn.connect(addr, port);
		CqlSession session = conn.getSession();

		
		RegularInsert insertInto = QueryBuilder.insertInto(Keyspace, StatisticsTable)
				.value("min_pickup_time", QueryBuilder.bindMarker())
				.value("max_pickup_time", QueryBuilder.bindMarker())
				.value("min_tip_amount", QueryBuilder.bindMarker())
				.value("max_tip_amount", QueryBuilder.bindMarker())
				.value("average_tip_amount", QueryBuilder.bindMarker())
				.value("trip_count", QueryBuilder.bindMarker())
				.value("top_payment_type_count", QueryBuilder.bindMarker())
				.value("top_payment_type_name", QueryBuilder.bindMarker())
				.value("top_rate_code_count", QueryBuilder.bindMarker())
				.value("top_rate_code_name", QueryBuilder.bindMarker());

		SimpleStatement insertStatement = insertInto.build();
		PreparedStatement preparedStatement = session.prepare(insertStatement);

		BoundStatement boundStatement = preparedStatement.bind()
				.setInstant(0, statisticRecord.getMinPickupTime().toInstant())
				.setInstant(1, statisticRecord.getMaxPickupTime().toInstant())
				.setFloat(2, statisticRecord.getMinTipAmount())
				.setFloat(3, statisticRecord.getMaxTipAmount())
				.setFloat(4, statisticRecord.getAverageTipAmount())
				.setLong(5, statisticRecord.getTripCount())
				.setInt(6, statisticRecord.getTopPaymentTypeCount())
				.setString(7, statisticRecord.getTopPaymentTypeName())
				.setInt(8, statisticRecord.getTopRateCodeCount())
				.setString(9, statisticRecord.getTopRateCodeName());

		session.execute(boundStatement);
		System.out.println(String.format("Data stored in %s table.", TaxiRecordsTable));
		
		session.close();
		conn.close();
	}

	/**
	 * Saves Taxi Record in Cassandra Db.
	 * @param taxiRecord
	 * @param addr
	 * @param port
	 */
    public static void saveTaxiRecord(TaxiRecord taxiRecord, String addr, Integer port) {
    	
        CassandraConnector conn = CassandraConnector.getInstance();
        conn.connect(addr, port);
        CqlSession session = conn.getSession();
        
        
        RegularInsert insertInto = QueryBuilder
            .insertInto(Keyspace, TaxiRecordsTable)
            .value("start_time", QueryBuilder.bindMarker())
            .value("end_time", QueryBuilder.bindMarker())
            .value("fare_amount", QueryBuilder.bindMarker())
            .value("pickup_latitude", QueryBuilder.bindMarker())
            .value("pickup_longitude", QueryBuilder.bindMarker())
            .value("mta_tax", QueryBuilder.bindMarker())
            .value("rate_code", QueryBuilder.bindMarker())
            .value("surcharge", QueryBuilder.bindMarker());

        SimpleStatement insertStatement = insertInto.build();
        PreparedStatement preapredStatement = session.prepare(insertStatement);

        BoundStatement boundStatement = preapredStatement.bind()
            .setInstant(0, taxiRecord.getStartTime().toInstant())
            .setInstant(1, taxiRecord.getEndTime().toInstant())
            .setFloat(2, taxiRecord.getFareAmount())
            .setDouble(3, taxiRecord.getPickupLatitude())
            .setDouble(4, taxiRecord.getPickupLongutude())
            .setFloat(5, taxiRecord.getMtaTax())
            .setString(6, taxiRecord.getRateCode())
            .setFloat(7, taxiRecord.getSurcharge());

        session.execute(boundStatement);
        conn.close();
    }

    /**
     * Reads data from Cassandra Db table.
     * @param tableName table from which the data is read
     * @param limit number of records to be read
     * @param addr
     * @param port
     */
	public static void readRecords(String tableName, int limit, String addr, Integer port) {
		
		CassandraConnector conn = CassandraConnector.getInstance();
		conn.connect(addr, port);
		CqlSession session = conn.getSession();
		
		
		String query = String.format("SELECT * FROM %s.%s limit %d" ,Keyspace, tableName, limit);
		ResultSet result = session.execute(query);
		
		System.out.println(String.format("First %d records from table %s", limit, tableName));
		System.out.println(result.all());
		
		session.close();
		conn.close();
	}
}
