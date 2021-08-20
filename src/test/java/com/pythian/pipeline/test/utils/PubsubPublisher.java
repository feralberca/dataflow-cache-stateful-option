package com.pythian.pipeline.test.utils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.util.List;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PubsubPublisher {

    public static void main(String[] args) throws Exception {
        String projectId = args[0];
        String topicId = args[1];
        publishLoop(projectId, topicId);
    }

    public static void publishLoop(String projectId, String topicId) throws Exception {
        TopicName topicName = TopicName.of(projectId, topicId);
        final Publisher publisher = Publisher.newBuilder(topicName).build();

        try {

            List<String> cities = Arrays.asList("{\n" + 
                                                " \"indoorTemp\":12.5,\n" + 
                                                " \"cityCode\":\"La Plata\",\n" + 
                                                " \"countryCode\":\"AR\"\n" + 
                                                " }",
                                                "{\n" + 
                                                " \"indoorTemp\":0.5,\n" + 
                                                " \"cityCode\":\"Ottawa\",\n" + 
                                                " \"countryCode\":\"CA\"\n" + 
                                                " }",
                                                "{\n" + 
                                                " \"indoorTemp\":-1,\n" + 
                                                " \"cityCode\":\"New York\",\n" + 
                                                " \"countryCode\":\"US\"\n" + 
                                                " }");

            AtomicBoolean loop = new AtomicBoolean(true);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    loop.set(false);
                    cleanUp(publisher);
                }
            });

            while (loop.get()) {

                int selectedIndex = Math.toIntExact(Math.round((cities.size()-1) * Math.random()));    
                ByteString data = ByteString.copyFromUtf8(cities.get(selectedIndex));
                
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                ApiFuture<String> future = publisher.publish(pubsubMessage);

                ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

                    @Override
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ApiException) {
                            ApiException apiException = ((ApiException) throwable);
                            // details on the API exception
                            System.out.println(apiException.getStatusCode().getCode());
                            System.out.println(apiException.isRetryable());
                        }
                        System.out.println("Error publishing message");
                    }

                    @Override
                    public void onSuccess(String messageId) {
                        // Once published, returns server-assigned message ids (unique within the topic)
                        System.out.println("Published message ID: " + messageId);
                    }
                }, MoreExecutors.directExecutor());
            }
        } finally {
            cleanUp(publisher);
        }
    }

    private static void cleanUp(Publisher publisher) {
        if (publisher != null) {
            publisher.shutdown();
            try {
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
    }
}
