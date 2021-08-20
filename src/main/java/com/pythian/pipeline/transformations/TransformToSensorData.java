package com.pythian.pipeline.transformations;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

import com.google.gson.Gson;
import com.pythian.pipeline.dto.InputSensorData;

public class TransformToSensorData extends DoFn<PubsubMessage, InputSensorData> {
	
	private static final long serialVersionUID = -713213532473432948L;

	private Gson gson;
	
	@DoFn.StartBundle
	public void startBundle(){
		gson = new Gson();
	}

	@DoFn.ProcessElement
	public void processElement(ProcessContext processContext) {
		//Do some crazy transformation
		PubsubMessage pubsubMessage = processContext.element();
		InputSensorData message = gson.fromJson(new String(pubsubMessage.getPayload()), InputSensorData.class);
		processContext.output(message);
	}

}
