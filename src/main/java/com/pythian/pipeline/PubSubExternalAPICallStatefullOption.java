package com.pythian.pipeline;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Default.Boolean;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.pythian.pipeline.dto.InputSensorData;
import com.pythian.pipeline.dto.OutputSensorData;
import com.pythian.pipeline.dto.WeatherData;
import com.pythian.pipeline.integrations.WeatherServiceWrapper;
import com.pythian.pipeline.transformations.TransformToSensorData;

/**
 * The {@link PubSubExternalAPICallStatefullOption} is a streaming pipeline which
 * use a stateful DoFn and Apache beam windowing functions for grouping and invoking 
 * external services on groups of objects requiring the same data
 *
 * <p>
 * <b>Pipeline Requirements</b>
 *
 * <ul>
 * <li>The Pub/Sub subscription must exist prior to pipeline execution.
 * <li>Output bucket must exists
 * </ul>
 *
 * <p>
 * <b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=<<< PROJECT_ID >>>
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pubsub-to-gcs-caching/pipelines
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.pythian.pipeline.PubSubExternalAPICallCaching \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --runner=${RUNNER} \
 * --subscription=SUBSCRIPTION \
 * --dataBucket=${PIPELINE_FOLDER}/input \
 * --maxStateInterval=60 \
 * --triggerEveryCount=3 \
 * --triggerEveryTimeSec=60 \
 * </pre>
 */
public class PubSubExternalAPICallStatefullOption {

    private static final Logger logger = LoggerFactory.getLogger(PubSubExternalAPICallStatefullOption.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the
     * executor at the command-line.
     */
    public interface Options extends PipelineOptions {

        @Description("The Pub/Sub subscription to read messages from")
        @Required
        String getSubscription();
        void setSubscription(String value);

        @Description("Dataflow bucket")
        String getDataBucket();
        void setDataBucket(String value);

        @Description("Cache enabled")
        @Boolean(true)
        boolean getCacheEnabled();
        void setCacheEnabled(boolean value);

        @Description("Maximun time in ms that objects are kept in storage before performing external service invocation")
        int getMaxStateInterval();
        void setMaxStateInterval(int value);

        @Description("Minimun number of elements to buffer before trigger processing")
        int getTriggerEveryCount();
        void setTriggerEveryCount(int value);

        @Description("Time elapsed before triggering processing in seconds")
        int getTriggerEveryTimeSec();
        void setTriggerEveryTimeSec(int value);
        
        @Description("Weather service API key")
        String getWeatherApiKey();
        void setWeatherApiKey(String value);        
    }

    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the
     * {@link PubSubExternalAPICallStatefullOption#run(Options)} method to start the
     * pipeline and invoke {@code result.waitUntilFinish()} on the
     * {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does
     * not wait until the pipeline is finished before returning. Invoke
     * {@code result.waitUntilFinish()} on the result object to block until the
     * pipeline is finished running if blocking programmatic execution is required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String outpath = options.getDataBucket() + "/output";
        String ingestSubscriptionPath = options.getSubscription();

        logger.info("Reading messages from subcription: {}", ingestSubscriptionPath);
        logger.info("Writting results to location: {}", outpath);

        options.setJobName(PubSubExternalAPICallStatefullOption.class.getSimpleName().toLowerCase() + "-"
                + sdf.format(new Date()));

        Pipeline pipeline = Pipeline.create(options);


        pipeline
            .apply("ReadMessages", PubsubIO.readMessagesWithAttributes().fromSubscription(ingestSubscriptionPath))
            .apply("Tranform", ParDo.of(new TransformToSensorData()))          
            .apply("MapMessageWithKey",
                MapElements
                    .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(InputSensorData.class)))
                    .via( (data) -> KV.of(data.getCityCode(), data)))          
            .apply("Window", 
                Window
                    .<KV<String, InputSensorData>>configure()
                    .triggering( Repeatedly.forever(AfterFirst.of(
                        AfterPane.elementCountAtLeast(options.getTriggerEveryCount()),
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getTriggerEveryTimeSec())))))
                    .discardingFiredPanes())
            .apply("EnrichWithExternalCall", 
            	   ParDo.of(new StatefulEnrichWithExternalCall(options.getMaxStateInterval(), options.getWeatherApiKey())))
            .apply("SaveToGcs", 
                TextIO.write().to(outpath)
                              .withTempDirectory(FileSystems.matchNewResource(outpath + "/temp", true))
                              .withWindowedWrites()
                              .withNumShards(1)
        );

        return pipeline.run();
    }
  
    public static class StatefulEnrichWithExternalCall extends DoFn<KV<String,InputSensorData>, String> {
        
        private static final long serialVersionUID = -9043112685508760474L;

        private Counter enrichedElementsCounter = Metrics.counter(StatefulEnrichWithExternalCall.class, "enriched-elements");
        private Counter apiCallCounter = Metrics.counter(StatefulEnrichWithExternalCall.class, "api-calls");
        
        @TimerId("STATE_TIMER")
        private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        @StateId("WEATHER_DATA")
        private final StateSpec<ValueState<WeatherData>> weatherStateSpec = StateSpecs.value(AvroCoder.of(WeatherData.class));
        
        @StateId("WEATHER_CITY")
        private final StateSpec<ValueState<String>> cityStateSpec =  StateSpecs.value();
        
        private Gson gson;
        
        private int maxStateInterval;
        
        private String weatherApiKey;
        
        private WeatherServiceWrapper weatherService;
        
        
        public StatefulEnrichWithExternalCall(int maxStateInterval, String weatherApiKey) {
            this.maxStateInterval = maxStateInterval;
            this.weatherApiKey = weatherApiKey;
        }
        
        @DoFn.StartBundle
        public void startBundle(){
            gson = new Gson();
            weatherService = new WeatherServiceWrapper(weatherApiKey);
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext processContext, 
        						   @StateId("WEATHER_DATA") ValueState<WeatherData> weatherState, 
        						   @StateId("WEATHER_CITY") ValueState<String> cityState, 
        						   @TimerId("STATE_TIMER") Timer timer) {
            enrichedElementsCounter.inc();
            KV<String, InputSensorData> kvmessage = processContext.element();
            InputSensorData message = kvmessage.getValue();
            WeatherData weatherData = weatherState.read();
            if (weatherData == null) {
                weatherData = retrieveFromApi(message.getCountryCode(), message.getCityCode());
                weatherState.write(weatherData);
                cityState.write(message.getCityCode());
                timer.offset(Duration.standardSeconds(maxStateInterval)).setRelative();
                logger.info("State initialized for city: {}", message.getCityCode());
            }
            OutputSensorData output = new OutputSensorData(message, weatherData);
            processContext.output(gson.toJson(output));
        }
        
        @OnTimer("STATE_TIMER")
        public void onTimer(OnTimerContext context, 
        					@StateId("WEATHER_DATA") ValueState<WeatherData> weatherData, 
        					@StateId("WEATHER_CITY") ValueState<String> cityState) {
            WeatherData data = weatherData.read();
            if (data != null) {
                weatherData.clear();
                logger.info("State cleaned for city: {}", cityState.read());
                cityState.clear();
            }
        }        

        @DoFn.Setup
        public void setup() throws Exception {

        }

        @DoFn.Teardown
        public void teardown() {

        }
        
        private WeatherData retrieveFromApi(String countryCode, String cityCode) {
            apiCallCounter.inc();
            return weatherService.get(countryCode, cityCode);
        }
    }
}