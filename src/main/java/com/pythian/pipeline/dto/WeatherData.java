package com.pythian.pipeline.dto;

import java.io.Serializable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@DefaultCoder(AvroCoder.class)
public class WeatherData implements Serializable {

	private static final long serialVersionUID = -4837019786779340608L;
	
	private Main main;
	private Wind wind;
	
	public WeatherData() {
		//default constructor for json serialization
	}
	
	public WeatherData(Main main, Wind wind) {
		this.main = main;
		this.wind = wind;
	}
	
	public Main getMain() {
		return main;
	}
	
	public void setMain(Main main) {
		this.main = main;
	}

	public Wind getWind() {
		return wind;
	}

	public void setWind(Wind wind) {
		this.wind = wind;
	}
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	@DefaultCoder(AvroCoder.class)
	public static class Main implements Serializable {

		private static final long serialVersionUID = -6468769691490155028L;
		
		private float temp;
		private float humidity;
		private float pressure;
		
		public Main() {
			//default constructor for json serialization
		}
		
		public Main(float temp, float humidity, float pressure) {
			this.temp = temp;
			this.humidity = humidity;
			this.pressure = pressure;
		}
		
		public float getTemp() {
			return temp;
		}
		
		public void setTemp(float temp) {
			this.temp = temp;
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
	}	
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	@DefaultCoder(AvroCoder.class)
	public static class Wind implements Serializable {
		
		private static final long serialVersionUID = -7969131767870081522L;
		
		private float speed;
		private int deg;
		
		public Wind() {
			//default constructor for json serialization
		}
		
		public Wind(float speed, int deg) {
			this.speed = speed;
			this.deg = deg;
		}
		
		public float getSpeed() {
			return speed;
		}
		
		public void setSpeed(float speed) {
			this.speed = speed;
		}
		
		public int getDeg() {
			return deg;
		}
		
		public void setDeg(int deg) {
			this.deg = deg;
		}
	}	
}