package models;

import java.io.Serializable;
import java.util.Date;

public class TaxiRecord implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Date startTime;
	private Date endTime;
	private Float fareAmount;
	private Double pickupLatitude;
	private Double pickupLongutude;
	private Float mtaTax;
	private String rateCode;
	private Float Surcharge;
	
	public TaxiRecord(Date startTime, Date endTime, Float fareAmount, Double pickupLatitude, Double pickupLongutude,
			Float mtaTax, String rateCode, Float surcharge) {
		super();
		this.startTime = startTime;
		this.endTime = endTime;
		this.fareAmount = fareAmount;
		this.pickupLatitude = pickupLatitude;
		this.pickupLongutude = pickupLongutude;
		this.mtaTax = mtaTax;
		this.rateCode = rateCode;
		Surcharge = surcharge;
	}
	
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	public Float getFareAmount() {
		return fareAmount;
	}
	public void setFareAmount(Float fareAmount) {
		this.fareAmount = fareAmount;
	}
	public Double getPickupLatitude() {
		return pickupLatitude;
	}
	public void setPickupLatitude(Double pickupLatitude) {
		this.pickupLatitude = pickupLatitude;
	}
	public Double getPickupLongutude() {
		return pickupLongutude;
	}
	public void setPickupLongutude(Double pickupLongutude) {
		this.pickupLongutude = pickupLongutude;
	}
	public Float getMtaTax() {
		return mtaTax;
	}
	public void setMtaTax(Float mtaTax) {
		this.mtaTax = mtaTax;
	}
	public String getRateCode() {
		return rateCode;
	}
	public void setRateCode(String rateCode) {
		this.rateCode = rateCode;
	}
	public Float getSurcharge() {
		return Surcharge;
	}
	public void setSurcharge(Float surcharge) {
		Surcharge = surcharge;
	}
	
	@Override
	public String toString() {
		return "TaxiRecord [startTime=" + startTime + ", endTime=" + endTime + ", fareAmount=" + fareAmount
				+ ", pickupLatitude=" + pickupLatitude + ", pickupLongutude=" + pickupLongutude + ", mtaTax=" + mtaTax
				+ ", rateCode=" + rateCode + ", Surcharge=" + Surcharge + "]";
	}
}
