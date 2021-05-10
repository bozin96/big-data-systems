import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class Main {

	public static void main(String[] args) {
		
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

		SparkSession spark = SparkSession.builder().appName("BigData-1").master(sparkMasterUrl).getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", "true").csv(csvFile);

		Dataset<Row> filteredDataset = dataset.filter(col("pickup_longitude").geq(-180).and(col("pickup_longitude").leq(180))
				  .and(col("dropoff_longitude").geq(-180)).and(col("dropoff_longitude").leq(180))
				  .and(col("pickup_latitude").geq(-90)).and(col("pickup_latitude").leq(90))
				  .and(col("dropoff_latitude").geq(-90)).and(col("dropoff_latitude").leq(90)));
		
		
		
		  showColumnStats(filteredDataset, "pickup_longitude");
		  showColumnStats(filteredDataset, "pickup_latitude");
		  showColumnStats(filteredDataset, "dropoff_longitude");
		  showColumnStats(filteredDataset, "dropoff_latitude");
		  showColumnStats(filteredDataset, "pickup_datetime");
		  showColumnStats(filteredDataset, "dropoff_datetime");
		  showColumnStats(filteredDataset, "fare_amount");
		  showColumnStats(filteredDataset, "total_amount");
		  showColumnStats(filteredDataset, "trip_distance");
		  
		  showColumnValueDistribution(filteredDataset, "passenger_count");
		  showColumnValueDistribution(filteredDataset, "payment_type");
		  
		  showNumberOfRidesInGivenTimePeriod(filteredDataset, "2014-01-09 20:45:25",
		  "2014-01-09 21:45:25");
		  
		  showTopBusiestHoursOfDay(filteredDataset, 7);
		  showTopBusiestDaysOfWeek(filteredDataset, 7);
		  
		  showTipAmountStatsOnLocation(filteredDataset, -73.991770000000003,
		  -74.994770000000003, 40.736828000000003, 40.736998000000003,
		  "2014-01-09 20:45:25", "2014-02-09 20:45:25");
		  
		  showTipAmountCountByRateCode(filteredDataset, -73.991770000000003,
		  -74.994770000000003, 40.736828000000003, 40.736998000000003,
		  "2014-01-09 20:45:25", "2014-02-09 20:45:25");
		 
		 

	    showAverageTripDurationByDayInWeak(filteredDataset);
		
		spark.stop();
		spark.close();
	}

	public static void showColumnValueDistribution(Dataset<Row> ds, String columnName) {

		ds.groupBy(columnName)
		  .agg(functions.count(ds.col(columnName)))
		  .show();
	}

	public static void showColumnStats(Dataset<Row> ds, String columnName) {

		Dataset<Row> columnSummary = ds.select(functions.min(ds.col(columnName)), 
											   functions.max(ds.col(columnName)),
										   	   functions.mean(ds.col(columnName)), 
										   	   functions.stddev(ds.col(columnName)));
		
		columnSummary.show();
	}

	static void showNumberOfRidesInGivenTimePeriod(Dataset<Row> ds, String startDateTime, String endDateTime) {

		Long count = ds.filter(ds.col("pickup_datetime").geq(startDateTime)
							   .and(ds.col("dropoff_datetime").leq(endDateTime)))
					   .count();
		
		System.out.println(
				String.format("Total number of rides between %s and %s is: %d", startDateTime, endDateTime, count));
	}

	public static void showTopBusiestHoursOfDay(Dataset<Row> ds, int limit) {
		
		if (limit > 24 || limit < 1)
			return;

		UDF1<String, Integer> udfGetDayOfWeak = Main::getDayOfWeekFromDate;
		UserDefinedFunction getDayOfWeak = udf(udfGetDayOfWeak, DataTypes.IntegerType);

		Dataset<Row> selectedData = ds.select(getDayOfWeak.apply(ds.col("pickup_datetime")).as("DayOfWeak"))
									  .groupBy("DayOfWeak")
									  .count()
									  .orderBy(col("count").desc())
									  .limit(limit);

		selectedData.show();
	}

	public static void showTopBusiestDaysOfWeek(Dataset<Row> ds, int limit) {
		
		if (limit > 7 || limit < 1)
			return;

		UDF1<String, Integer> udfGetHourOfDay = Main::getHourOfDayFromDate;
		UserDefinedFunction getHourOfDay = udf(udfGetHourOfDay, DataTypes.IntegerType);

		Dataset<Row> selectedData = ds.select(getHourOfDay.apply(ds.col("pickup_datetime")).as("HourOfDay"))
									  .groupBy("HourOfDay")
									  .count()
									  .orderBy(col("count").desc())
									  .limit(limit);

		selectedData.show();
	}

	public static void showAverageTripDurationByDayInWeak(Dataset<Row> ds)
	{
		UDF1<String, Integer> udfGetDayOfWeak = Main::getDayOfWeekFromDate;
		UserDefinedFunction getDayOfWeak = udf(udfGetDayOfWeak, DataTypes.IntegerType);
		

		UDF2<String, String, Long> udfGetTripDuration = Main::geTripDurationTime;
		UserDefinedFunction getTripDuration = udf(udfGetTripDuration, DataTypes.LongType);
		
		
	    ds.select(getDayOfWeak.apply(ds.col("pickup_datetime")).as("StartingDayOfWeak"),
	    		  getDayOfWeak.apply(ds.col("dropoff_datetime")).as("EndingDayOfWeak"),
	    		  getTripDuration.apply(ds.col("pickup_datetime"), ds.col("dropoff_datetime")).as("TripDuration"))
	    .filter(col("StartingDayOfWeak").equalTo(col("EndingDayOfWeak")))
	    .groupBy("StartingDayOfWeak")
	    .agg(avg("TripDuration"))
	    .show();
	}

	public static Long geTripDurationTime(String pickupDateTime, String dropoffDateTime) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date pickupDate = null; 
		Date dropoffDate = null;

		try {
			pickupDate = formatter.parse(pickupDateTime);
		} catch (ParseException e) {
			pickupDate = new Date();
			e.printStackTrace();
		}
		
		try {
			dropoffDate = formatter.parse(dropoffDateTime);
		} catch (ParseException e) {
			dropoffDate = new Date();
			e.printStackTrace();
		}
		
        long diffInMillies = dropoffDate.getTime() - pickupDate.getTime();
        return TimeUnit.MINUTES.convert(diffInMillies,TimeUnit.MILLISECONDS);
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

	public static void showTipAmountCountByRateCode(Dataset<Row> ds, double minLongitude, double maxLongitude,
			double minLatitude, double maxLatitude, String startTime, String endTime) {
		
		ds.filter(col("pickup_longitude").geq(minLongitude)
				  .and(col("dropoff_longitude").leq(maxLongitude))
				  .and(col("pickup_latitude").geq(minLatitude))
				  .and(col("dropoff_latitude").leq(maxLatitude))
				  .and(col("pickup_datetime").geq(lit(startTime)))
				  .and(col("dropoff_datetime").leq(lit(endTime))))
		  .groupBy(col("rate_code"))
		  .agg(count(ds.col("tip_amount").gt(0.0)))
		  .show();
	}

	public static void showTipAmountStatsOnLocation(Dataset<Row> ds, double minLongitude, double maxLongitude,
			double minLatitude, double maxLatitude, String startTime, String endTime) {
		
		ds.filter(col("pickup_longitude").geq(minLongitude)
				.and(col("dropoff_longitude").leq(maxLongitude))
				.and(col("pickup_latitude").geq(minLatitude))
				.and(col("dropoff_latitude").leq(maxLatitude))
				.and(col("pickup_datetime").geq(functions.lit(startTime)))
				.and(col("dropoff_datetime").leq(functions.lit(endTime))))
				.select(functions.min("tip_amount"), functions.max("tip_amount"), 
						functions.mean("tip_amount"), functions.stddev("tip_amount"))
		 .show();

	}
}
