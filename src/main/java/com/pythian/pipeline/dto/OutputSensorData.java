package com.pythian.pipeline.dto;

import java.io.Serializable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class OutputSensorData implements Serializable {

	private static final long serialVersionUID = -7744174528293696113L;
	
	private String cityCode;
	private String countryCode;
	private float indoorTemp;
	private float outdoorTemp;
	private float humidity;
	private float pressure;
	private float speed;
	private int direction;
	
	public OutputSensorData(InputSensorData input, WeatherData weatherData) {
		
		this.setIndoorTemp(input.getIndoorTemp());
		this.setCityCode(input.getCityCode());
		this.setCountryCode(input.getCountryCode());
		
		this.setOutdoorTemp(weatherData.getMain().getTemp());
		this.setHumidity(weatherData.getMain().getHumidity());
		this.setPressure(weatherData.getMain().getPressure());
		this.setSpeed(weatherData.getWind().getSpeed());
		this.setDirection(weatherData.getWind().getDeg());			
	}

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

	public float getOutdoorTemp() {
		return outdoorTemp;
	}
	
	public void setOutdoorTemp(float outdoorTemp) {
		this.outdoorTemp = outdoorTemp;
	}
	
	public float getHumidity() {
		return humidity;
	}
	
	public void setHumidity(float humidity) {
		this.humidity = humidity;
	}
	
	public float getPressure() {
		return pressure;
	}
	
	public void setPressure(float pressure) {
		this.pressure = pressure;
	}
	
	public float getSpeed() {
		return speed;
	}
	
	public void setSpeed(float speed) {
		this.speed = speed;
	}
	
	public int getDirection() {
		return direction;
	}
	
	public void setDirection(int direction) {
		this.direction = direction;
	}		
}
