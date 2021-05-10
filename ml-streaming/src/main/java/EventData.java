public class EventData {

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
    private final String pickupDateTime;
    private final String dropoffDateTime;
    private final String passengerCount;
    private final String tripDistance;
    private final String pickupLongitute;
    private final String pickupLatitude;
    private final String rateCode;
    private final String storeAndFwdFlag;
    private final String dropoffLongitude;
    private final String dropoffLatitude;
    private final String paymentType;
    private final String fareAmount;
    private final String surcharge;
    private final String mtaTax;
    private final String tipAmount;
    private final String tollsAmount;
    private final String totalAmount;


    private EventData(String[] args) {
        this.vendorId = args[0];
        this.pickupDateTime = args[1];
        this.dropoffDateTime = args[2];
        this.passengerCount = args[3];
        this.tripDistance = args[4];
        this.pickupLongitute = args[5];
        this.pickupLatitude = args[6];
        this.rateCode = args[7];
        this.storeAndFwdFlag = args[8];
        this.dropoffLongitude = args[9];
        this.dropoffLatitude = args[10];
        this.paymentType = args[11];
        this.fareAmount = args[12];
        this.surcharge = args[13];
        this.mtaTax = args[14];
        this.tipAmount =  args[15];
        this.tollsAmount = args[16];
        this.totalAmount = args[17];
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", vendorId, pickupDateTime,
                dropoffDateTime, passengerCount, tripDistance, pickupLongitute, pickupLatitude, rateCode, storeAndFwdFlag, dropoffLongitude, dropoffLatitude, paymentType,
                fareAmount, surcharge, mtaTax, tipAmount, tollsAmount, totalAmount
        );
    }

	public String getVendorId() {
		return vendorId;
	}

	public String getPickupDateTime() {
		return pickupDateTime;
	}

	public String getDropoffDateTime() {
		return dropoffDateTime;
	}

	public String getPassengerCount() {
		return passengerCount;
	}

	public String getTripDistance() {
		return tripDistance;
	}

	public String getPickupLongitute() {
		return pickupLongitute;
	}

	public String getPickupLatitude() {
		return pickupLatitude;
	}

	public String getRateCode() {
		return rateCode;
	}

	public String getStoreAndFwdFlag() {
		return storeAndFwdFlag;
	}

	public String getDropoffLongitude() {
		return dropoffLongitude;
	}

	public String getDropoffLatitude() {
		return dropoffLatitude;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public String getFareAmount() {
		return fareAmount;
	}

	public String getSurcharge() {
		return surcharge;
	}

	public String getMtaTax() {
		return mtaTax;
	}

	public String getTipAmount() {
		return tipAmount;
	}

	public String getTollsAmount() {
		return tollsAmount;
	}

	public String getTotalAmount() {
		return totalAmount;
	}

}
