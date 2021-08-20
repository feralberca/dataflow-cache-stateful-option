package com.pythian.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import com.google.gson.Gson;
import com.pythian.pipeline.PubSubExternalAPICallStatefullOption.StatefulEnrichWithExternalCall;
import com.pythian.pipeline.dto.InputSensorData;
import com.pythian.pipeline.dto.OutputSensorData;
import com.pythian.pipeline.transformations.TransformToSensorData;

public class PubSubExternalAPICallStatefulTransformTest {
	
	@Rule public final transient TestPipeline pipeline = TestPipeline.create();

	@Test
	public void testPipeline() {
		
		String weatherApiKey = System.getProperty("weather-api-key");
		Objects.requireNonNull(weatherApiKey, "Missing required system property 'weather-api-key'");
		
		String payload = "{\n" + 
						 " \"indoorTemp\":12.5,\n" + 
						 " \"cityCode\":\"La Plata\",\n" + 
						 " \"countryCode\":\"AR\"\n" + 
						 " }";

	    PubsubMessage message = new PubsubMessage(payload.getBytes(), Collections.emptyMap());

		PCollection<String> results = 
			pipeline
				.apply(Create.of(message))
				.apply("Tranform", ParDo.of(new TransformToSensorData()))
	            .apply("MapMessageWithKey",
	                    MapElements
	                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(InputSensorData.class)))
	                        .via( (data) -> KV.of(data.getCityCode(), data)))   				
				.apply("EnrichWithExternalCall", ParDo.of(new StatefulEnrichWithExternalCall(60, weatherApiKey)));
		
		PAssert.that(results).satisfies((sensorDataStringIterable) -> {
			
			Gson gson = new Gson();
			StreamSupport.stream(sensorDataStringIterable.spliterator(), false).forEach(sensorDataString -> {
				OutputSensorData data = gson.fromJson(sensorDataString, OutputSensorData.class);
				assertThat(data.getCityCode(), is(notNullValue()));
				assertThat(data.getOutdoorTemp(), is(greaterThan(0f)));
			});
				
		    return null;
		});

	    pipeline.run();
	}
}
