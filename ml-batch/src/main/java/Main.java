import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.apache.spark.sql.types.DataTypes.*;

public class Main {
    public static void main(String[] args) throws IOException {
    	
        if (args.length < 1) {
            throw new IllegalArgumentException("Csv file path on the hdfs must be passed as argument");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        String hdfsPath = args[0];
        String csvFile = hdfsUrl + hdfsPath;

        SparkSession spark = SparkSession.builder().appName("BigData-3-ML-Saving").master(sparkMasterUrl).getOrCreate();

        Dataset<Row> dataSet = spark.read().option("header", "true").csv(csvFile);
        
        Dataset<Row> filteredData = dataSet.filter((row) -> {
            return !row.anyNull();
        });
        
        UDF1<String, Integer> udfGetDayOfWeak = Main::getDayOfWeekFromDate;
        UserDefinedFunction getDayOfWeak = functions.udf(udfGetDayOfWeak, DataTypes.IntegerType);

        UDF1<String, Integer> udfGetHourOfDay = Main::getHourOfDayFromDate;
        UserDefinedFunction getHourOfDay = functions.udf(udfGetHourOfDay, DataTypes.IntegerType);
        
        Dataset<Row> selectedData = filteredData.select(
        		dataSet.col("trip_distance").cast(FloatType).as("TripDistance"),
        		dataSet.col("pickup_longitude").cast(DoubleType).as("PickupLongitude"),
        		dataSet.col("pickup_latitude").cast(DoubleType).as("PickupLatitude"),
        		dataSet.col("dropoff_longitude").cast(DoubleType).as("DropoffLongitude"),
        		dataSet.col("dropoff_latitude").cast(DoubleType).as("DropoffLatitude"),
        		dataSet.col("rate_code").cast(IntegerType).as("RateCode"),
        		getDayOfWeak.apply(dataSet.col("pickup_datetime")).as("DayOfWeak"),
        		getHourOfDay.apply(dataSet.col("pickup_datetime")).as("HourOfDay"),
        		dataSet.col("fare_amount").cast(FloatType).as("FareAmount")
        		);
        
        // nyc coordinates:
        // latitude = 40.730610
        // longitude = -73.935242
        double nycMinLatitude = 38.730610; // -2
        double nycMaxLatitude = 42.730610; // +2
        double nycMinLongitude = -75.935242; // -2
        double nycMaxLongitude = -71.935242; // +2
        
        Dataset<Row> filteredAndSelectedData = selectedData.filter(selectedData.col("TripDistance").notEqual(0.0)
        			.and(selectedData.col("PickupLongitude").gt(nycMinLongitude))
        			.and(selectedData.col("PickupLongitude").lt(nycMaxLongitude))
        			.and(selectedData.col("PickupLatitude").gt(nycMinLatitude))
        			.and(selectedData.col("PickupLatitude").lt(nycMaxLatitude))
        			.and(selectedData.col("DropoffLongitude").gt(nycMinLongitude))
        			.and(selectedData.col("DropoffLongitude").lt(nycMaxLongitude))
        			.and(selectedData.col("DropoffLatitude").gt(nycMinLatitude))
        			.and(selectedData.col("DropoffLatitude").lt(nycMaxLatitude)));
        		
        
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"TripDistance", "PickupLongitude", "PickupLatitude", "DropoffLongitude",
                		"DropoffLatitude","DayOfWeak", "HourOfDay","RateCode"})
                .setOutputCol("Features");

        Dataset<Row> transformedData = vectorAssembler.transform(filteredAndSelectedData);

        Dataset<Row>[] splits = transformedData.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("FareAmount")
                .setFeaturesCol("Features");
        
        RandomForestRegressionModel model = rf.fit((trainingData));

        model.write().overwrite().save(hdfsUrl + "/big-data/ml-model");

        Dataset<Row> predictions = model.transform(testData);
        predictions.show(100);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("FareAmount")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

        spark.stop();
        spark.close();
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
