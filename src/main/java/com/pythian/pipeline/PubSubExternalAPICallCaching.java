package com.pythian.pipeline;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import com.google.gson.Gson;
import net.spy.memcached.MemcachedClient;
import com.pythian.pipeline.dto.InputSensorData;
import com.pythian.pipeline.dto.OutputSensorData;
import com.pythian.pipeline.dto.WeatherData;
import com.pythian.pipeline.integrations.WeatherServiceWrapper;
import com.pythian.pipeline.transformations.TransformToSensorData;

/**
 * The {@link PubSubExternalAPICallCaching} is a streaming pipeline which
 * invokes an expensive external service on each processed element that shows an 
 * strategy for caching that result in a distributed caching solution among workers
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
 * CACHE_ENDPOINT=<<<MEMCACHED ENDPOINT AND PORT>>>
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
 * --dataBucket=${PIPELINE_FOLDER}/input 
 * --cacheEndpoint=${CACHE_ENDPOINT} \
 * --cacheTTL=60 \
 * </pre>
 */
public class PubSubExternalAPICallCaching {

    private static final Logger logger = LoggerFactory.getLogger(PubSubExternalAPICallCaching.class);

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

        @Description("CacheEndpoint")
        String getCacheEndpoint();
        void setCacheEndpoint(String value);

        @Description("Cache TTL in seconds")
        @Boolean(true)
        int getCacheTTL();
        void setCacheTTL(int value);
        
        @Description("Weather service API key")
        String getWeatherApiKey();
        void setWeatherApiKey(String value);
        
    }

    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the
     * {@link PubSubExternalAPICallCaching#run(Options)} method to start the
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

        options.setJobName(
                PubSubExternalAPICallCaching.class.getSimpleName().toLowerCase() + "-" + sdf.format(new Date()));

        Pipeline pipeline = Pipeline.create(options);

        pipeline
              .apply("ReadMessages", PubsubIO.readMessagesWithAttributes().fromSubscription(ingestSubscriptionPath))
              .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(20))))
              .apply("Tranform", ParDo.of(new TransformToSensorData()))
              .apply("EnrichWithExternalCall", 
            		  ParDo.of(new EnrichWithExternalCall(
            				  		options.getCacheEnabled(), 
            				  		options.getCacheEndpoint(), 
            				  		options.getCacheTTL(),
            				  		options.getWeatherApiKey())))
              .apply("SaveToGcs", 
                     TextIO.write().to(outpath)
                                   .withTempDirectory(FileSystems.matchNewResource(outpath + "/temp", true))
                                   .withWindowedWrites()
                                   .withNumShards(1)
        );
    
        return pipeline.run();
    }

    public static class EnrichWithExternalCall extends DoFn<InputSensorData, String> {

        private static final long serialVersionUID = -9043112685508760474L;

        private Counter enrichedElementsCounter = Metrics.counter(EnrichWithExternalCall.class, "enriched-elements");
        private Counter apiCallCounter = Metrics.counter(EnrichWithExternalCall.class, "api-calls");

        private Gson gson;
        private MemcachedClient cacheInstance;

        private boolean cacheEnabled;
        private String cacheEndpoint;
        private int cacheTTL;
        private String weatherApiKey;

        private WeatherServiceWrapper weatherService;

        public EnrichWithExternalCall(boolean cacheEnabled, String cacheEndpoint, int cacheTTL, String weatherApiKey) {
            this.cacheEnabled = cacheEnabled;
            this.cacheEndpoint = cacheEndpoint;
            this.cacheTTL = cacheTTL;
            this.weatherApiKey = weatherApiKey;
        }

        @DoFn.StartBundle
        public void startBundle() {
            gson = new Gson();
            weatherService = new WeatherServiceWrapper(weatherApiKey);
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext processContext) {
            enrichedElementsCounter.inc();
            InputSensorData message = processContext.element();
            WeatherData weatherData = getWeatherForCity(message.getCountryCode(), message.getCityCode());
            OutputSensorData output = new OutputSensorData(message, weatherData);
            processContext.output(gson.toJson(output));
        }

        @DoFn.Setup
        public void setup() throws Exception {
            if (cacheEnabled) {
                cacheInstance = createCacheClusterMember();
            }
        }

        @DoFn.Teardown
        public void teardown() {

        }
        
        private WeatherData getWeatherForCity(String countryCode, String cityCode) {
            if (cacheEnabled) {
                String cacheKey = Base64.getEncoder().encodeToString(String.format("%s-%s", countryCode, cityCode).getBytes());
                WeatherData weatherData = (WeatherData) cacheInstance.get(cacheKey);
                if (weatherData == null) {
                    weatherData = retrieveFromApi(countryCode, cityCode);
                    cacheInstance.set(cacheKey, cacheTTL, weatherData);
                }
                return weatherData; 
            } else {
                return retrieveFromApi(countryCode, cityCode);
            }
        }
        
        private WeatherData retrieveFromApi(String countryCode, String cityCode) {
            apiCallCounter.inc();
            return weatherService.get(countryCode, cityCode);
        }
        
        private MemcachedClient createCacheClusterMember() throws Exception {
            String[] endPointTokens = cacheEndpoint.split(":");
            if (endPointTokens.length != 2) {
                throw new IllegalArgumentException(
                			String.format("Invalid cache endpoint format: '%s', expected format: 'host:port'", cacheEndpoint)
                		);
            }
            MemcachedClient instance = 
            		new MemcachedClient(new InetSocketAddress(endPointTokens[0], Integer.parseInt(endPointTokens[1])));
            logger.info("Cache client initialized");
            return instance;
        }
    }
}