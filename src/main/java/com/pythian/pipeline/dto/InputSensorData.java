package com.pythian.pipeline.dto;

import java.io.Serializable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class InputSensorData implements Serializable {
	
	private static final long serialVersionUID = 3303302727344247286L;
	
	private float indoorTemp;
	private String cityCode;
	private String countryCode;
	
	public float getIndoorTemp() {
		return indoorTemp;
	}
	
	public void setIndoorTemp(float indoorTemp) {
		this.indoorTemp = indoorTemp;
	}
	
	public String getCityCode() {
		return cityCode;
	}
	
	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}
	
	public String getCountryCode() {
		return countryCode;
	}
	
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
}