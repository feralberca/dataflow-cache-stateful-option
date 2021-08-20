package com.pythian.pipeline.integrations;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.pythian.pipeline.dto.WeatherData;

public class WeatherServiceWrapper {
	
	private Client client;
	private String apiKey;
	
	public WeatherServiceWrapper(String apiKey) {
		client = ClientBuilder.newClient(); 
		this.apiKey = apiKey;
	}
	
	public WeatherData get(String countryCode, String cityCode) {

		// api.openweathermap.org/data/2.5/weather?q={city name},{country code}
		WebTarget target = client.target("http://api.openweathermap.org/data/2.5/weather")
				.queryParam("q", cityCode + "," + countryCode)
				.queryParam("APPID", apiKey)
				.queryParam("cnt", "10")
				.queryParam("mode", "json")
				.queryParam("units", "metric");

		return target.request(MediaType.APPLICATION_JSON).get(WeatherData.class);
	}

}
