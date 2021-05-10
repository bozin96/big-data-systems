package models;

import java.io.Serializable;
import java.util.Date;

public class StatisticRecord implements Serializable {

	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private Date minPickupTime;
    private Date maxPickupTime;
    private Float minTipAmount;
    private Float maxTipAmount;
    private Float averageTipAmount;
    private Long tripCount;
    private Integer topPaymentTypeCount;
    private String topPaymentTypeName;
    private Integer topRateCodeCount;
    private String topRateCodeName;
    

	public StatisticRecord(Date minPickupTime, Date maxPickupTime, Float minTipAmount, Float maxTipAmount, Float averageTipAmount,
			Long tripCount, Integer topPaymentTypeCount, String topPaymentTypeName,
			Integer topRateCodeCount,String topRateCodeName) {
		super();
		this.minPickupTime = minPickupTime;
		this.maxPickupTime = maxPickupTime;
		this.minTipAmount = minTipAmount;
		this.maxTipAmount = maxTipAmount;
		this.averageTipAmount = averageTipAmount;
		this.tripCount = tripCount;
		this.topPaymentTypeCount = topPaymentTypeCount;
		this.topPaymentTypeName = topPaymentTypeName;
		this.topRateCodeCount = topRateCodeCount;
		this.topRateCodeName = topRateCodeName;
	}

	public Date getMinPickupTime() {
		return minPickupTime;
	}
	
	public void setMinPickupTime(Date minPickupTime) {
		this.minPickupTime = minPickupTime;
	}
	
	public Date getMaxPickupTime() {
		return maxPickupTime;
	}
	
	public void setMaxPickupTime(Date maxPickupTime) {
		this.maxPickupTime = maxPickupTime;
	}
	
	public Float getMinTipAmount() {
		return minTipAmount;
	}
	
	public void setMinTipAmount(Float minTipAmount) {
		this.minTipAmount = minTipAmount;
	}
	
	public Float getMaxTipAmount() {
		return maxTipAmount;
	}
	
	public void setMaxTipAmount(Float maxTipAmount) {
		this.maxTipAmount = maxTipAmount;
	}
	
	public Float getAverageTipAmount() {
		return averageTipAmount;
	}
	
	public void setAverageTipAmount(Float averageTipAmount) {
		this.averageTipAmount = averageTipAmount;
	}
	
	public Long getTripCount() {
		return tripCount;
	}
	
	public void setTripCount(Long tripCount) {
		this.tripCount = tripCount;
	}
	
	public Integer getTopPaymentTypeCount() {
		return topPaymentTypeCount;
	}

	public void setTopPaymentTypeCount(Integer topPaymentTypeCount) {
		this.topPaymentTypeCount = topPaymentTypeCount;
	}

	public String getTopPaymentTypeName() {
		return topPaymentTypeName;
	}

	public void setTopPaymentTypeName(String topPaymentTypeName) {
		this.topPaymentTypeName = topPaymentTypeName;
	}

	public Integer getTopRateCodeCount() {
		return topRateCodeCount;
	}

	public void setTopRateCodeCount(Integer topRateCodeCount) {
		this.topRateCodeCount = topRateCodeCount;
	}

	public String getTopRateCodeName() {
		return topRateCodeName;
	}

	public void setTopRateCodeName(String topRateCodeName) {
		this.topRateCodeName = topRateCodeName;
	}

	@Override
	public String toString() {
		return "StatisticData [minPickupTime=" + minPickupTime + ", maxPickupTime=" + maxPickupTime + ", minTipAmount="
				+ minTipAmount + ", maxTipAmount=" + maxTipAmount + ", tripCount=" + tripCount + "]";
	}
}
