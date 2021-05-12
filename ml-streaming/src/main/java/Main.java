import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.createStructField;

public class Main {
    private static final String TaxiTopic = "taxi-data";

    public static void main(String[] args) throws Exception {

        System.out.println("RUNNING ML-STREAMING");
        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
        if (initialSleepTime != null && !initialSleepTime.equals("")) {
            int sleep = Integer.parseInt(initialSleepTime);
            System.out.println("Sleeping on start " + sleep + "sec");
            Thread.sleep(sleep * 1000);
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
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

        SparkSession spark = SparkSession.builder().appName("BigData-4-ML-Streaming").master(sparkMasterUrl).getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext, new Duration(dataReceivingSleep * 1000));

        RandomForestRegressionModel model = RandomForestRegressionModel.load(hdfsUrl + "/big-data/ml-model/");

        Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
        Collection<String> topics = Collections.singletonList(TaxiTopic);

        JavaInputDStream<ConsumerRecord<Object, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);
        JavaDStream<EventData> eventData = receivedData.map(EventData::CreateEventData);
        
        JavaDStream<EventData> filteredData = eventData.filter(ed -> ed != null && 
        		!ed.getTripDistance().equals("0.0") &&
        		!ed.getPickupLongitute().equals("0.0") &&
        		!ed.getPickupLatitude().equals("0.0") && 
        		!ed.getDropoffLongitude().equals("0.0") && 
        		!ed.getDropoffLatitude().equals("0.0") && 
        		!ed.getRateCode().equals(null) && 
        		!ed.getPickupDateTime().equals(null) 
        );

        JavaDStream<Row> rows = filteredData.map(row -> RowFactory.create(
        		Float.parseFloat(row.getTripDistance()),
        		Double.parseDouble(row.getPickupLongitute()),
        		Double.parseDouble(row.getPickupLatitude()),
        		Double.parseDouble(row.getDropoffLongitude()),
        		Double.parseDouble(row.getDropoffLatitude()),
        		Integer.parseInt(row.getRateCode()),
        		getDayOfWeekFromDate(row.getPickupDateTime()),
        		getHourOfDayFromDate(row.getPickupDateTime())));
        
        rows.foreachRDD(d -> {
            StructType rowSchema = DataTypes.createStructType(
                    new StructField[]{
                            createStructField("TripDistance", DataTypes.FloatType, false),
                            createStructField("PickupLongitude", DataTypes.DoubleType, false),
                            createStructField("PickupLatitude", DataTypes.DoubleType, false),
                            createStructField("DropoffLongitude", DataTypes.DoubleType, false),
                            createStructField("DropoffLatitude", DataTypes.DoubleType, false),
                            createStructField("RateCode", DataTypes.IntegerType, false),
                            createStructField("DayOfWeak", DataTypes.IntegerType, false),
                            createStructField("HourOfDay", DataTypes.IntegerType, false),
                    });

            Dataset<Row> data = spark.createDataFrame(d, rowSchema);

            VectorAssembler vectorAssembler = new VectorAssembler()
                    .setInputCols(new String[]{"TripDistance", "PickupLongitude", "PickupLatitude", "DropoffLongitude",
                    		"DropoffLatitude", "RateCode", "DayOfWeak", "HourOfDay"})
                    .setOutputCol("Features");

            Dataset<Row> transformed = vectorAssembler.transform(data);
            Dataset<Row> predictions = model.transform(transformed);
            predictions.show(100);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Map<String, Object> getKafkaParams(String brokers) {
        Map<String, Object> kafkaParams = new HashMap<>();
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

    public static int getDayOfWeekFromDate(String stringDate) {
		// example format: 2014-01-09 20:45:25
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		
		try {
			date = formatter.parse(stringDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_WEEK); // the day of the week in numerical format
    }
    
    public static int getHourOfDayFromDate(String stringDate) {
		// example format: 2014-01-09 20:45:25
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		
		try {
			date = formatter.parse(stringDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY); // the hour of the day in numerical format
    }
    
}
