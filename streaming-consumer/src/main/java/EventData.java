import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static EventData CreateEventData(String data) throws Exception {
		String[] args = data.split(",");

		if (args.length == 17) {
			String newData = data + " ";
			args = newData.split(",");
		}

		if (args.length < 18) {
			return null;
		}

		return new EventData(args);
	}

	// vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,
	// pickup_latitude,rate_code,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,
	// surcharge,mta_tax,tip_amount,tolls_amount,total_amount

	private final String vendorId;
	private Date pickupDateTime;
	private Date dropoffDateTime;
	private final String passengerCount;
	private final String tripDistance;
	private Double pickupLongitute;
	private Double pickupLatitude;
	private final String rateCode;
	private final String storeAndFwdFlag;
	private Double dropoffLongitude;
	private Double dropoffLatitude;
	private final String paymentType;
	private Float fareAmount;
	private Float surcharge;
	private Float mtaTax;
	private Float tipAmount;
	private final String tollsAmount;
	private final String totalAmount;

	private EventData(String[] args) {
		// 2014-01-09 20:45:25
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		this.vendorId = args[0];

		try {
			this.pickupDateTime = formatter.parse(args[1]);
		} catch (ParseException e) {
			this.pickupDateTime = new Date();
			e.printStackTrace();
		}

		try {
			this.dropoffDateTime = formatter.parse(args[2]);
		} catch (ParseException e) {
			this.dropoffDateTime = new Date();
			e.printStackTrace();
		}
		
		this.passengerCount = args[3];
		this.tripDistance = args[4];
		this.pickupLongitute = com.google.common.primitives.Doubles.tryParse(args[5]);
		if (this.pickupLongitute == null)
			this.pickupLongitute = (double) 0.0;

		this.pickupLatitude = com.google.common.primitives.Doubles.tryParse(args[6]);
		if (this.pickupLatitude == null)
			this.pickupLatitude = (double) 0.0;
		this.rateCode = args[7];
		this.storeAndFwdFlag = args[8];
		
		this.dropoffLongitude = com.google.common.primitives.Doubles.tryParse(args[9]);
		if (this.dropoffLongitude == null)
			this.dropoffLongitude = (double) 0.0;
		
		this.dropoffLatitude = com.google.common.primitives.Doubles.tryParse(args[10]);
		if (this.dropoffLatitude == null)
			this.dropoffLatitude = (double) 0.0;
		
		this.paymentType = args[11];
		
		this.fareAmount = com.google.common.primitives.Floats.tryParse(args[12]);
		if (this.fareAmount == null)
			this.fareAmount = (float) 0.0;
		
		this.surcharge = com.google.common.primitives.Floats.tryParse(args[13]);
		if (this.surcharge == null)
			this.surcharge = (float) 0.0;
		
		this.mtaTax = com.google.common.primitives.Floats.tryParse(args[14]);
		if (this.mtaTax == null)
			this.mtaTax = (float) 0.0;
		
		this.tipAmount = com.google.common.primitives.Floats.tryParse(args[15]);
		if (this.tipAmount == null)
			this.tipAmount = (float) 0.0;
		this.tollsAmount = args[16];
		this.totalAmount = args[17];
	}

	@Override
	public String toString() {
		return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", vendorId, pickupDateTime,
				dropoffDateTime, passengerCount, tripDistance, pickupLongitute, pickupLatitude, rateCode,
				storeAndFwdFlag, dropoffLongitude, dropoffLatitude, paymentType, fareAmount, surcharge, mtaTax,
				tipAmount, tollsAmount, totalAmount);
	}

	public String getVendorId() {
		return vendorId;
	}

	public Date getPickupDateTime() {
		return pickupDateTime;
	}

	public Date getDropoffDateTime() {
		return dropoffDateTime;
	}

	public String getPassengerCount() {
		return passengerCount;
	}

	public String getTripDistance() {
		return tripDistance;
	}

	public Double getPickupLongitute() {
		return pickupLongitute;
	}

	public Double getPickupLatitude() {
		return pickupLatitude;
	}

	public String getRateCode() {
		return rateCode;
	}

	public String getStoreAndFwdFlag() {
		return storeAndFwdFlag;
	}

	public Double getDropoffLongitude() {
		return dropoffLongitude;
	}

	public Double getDropoffLatitude() {
		return dropoffLatitude;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public Float getFareAmount() {
		return fareAmount;
	}

	public Float getSurcharge() {
		return surcharge;
	}

	public Float getMtaTax() {
		return mtaTax;
	}

	public Float getTipAmount() {
		return tipAmount;
	}

	public String getTollsAmount() {
		return tollsAmount;
	}

	public String getTotalAmount() {
		return totalAmount;
	}

}
