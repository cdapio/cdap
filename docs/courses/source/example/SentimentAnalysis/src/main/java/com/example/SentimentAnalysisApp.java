package com.example;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Property;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;

import com.continuuity.flow.flowlet.ExternalProgramFlowlet;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Application that analyzes sentiment of sentences as positive, negative or neutral.
 */
public class SentimentAnalysisApp implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisApp.class);

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
    .setName("SentimentAnalysisApp")
    .setDescription("Application for Sentiment Analysis")
    .withStreams()
      .add(new Stream("sentence"))
    .withDataSets()
      .add(new Table("sentiments"))
      .add(new SimpleTimeseriesTable("text-sentiments"))
    .withFlows()
      .add(new SentimentAnalysisFlow())
    .withProcedures()
      .add(new SentimentAnalysisProcedure())
    .noMapReduce()
    .noWorkflow()
//    .withMapReduce()
//        .add(new SentimentAnalysisMapReduce())
//    .withWorkflows()
//        .add(new SentimentAnalysisWorkflow())
    .build();
  }
  
  /**
   * Flow for sentiment analysis.
   */
  public static class SentimentAnalysisFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
      .setName("analysis")
      .setDescription("Analysis of text to generate sentiments")
      .withFlowlets()
        .add(new Normalization())
        .add(new Analyze())
        .add(new Update())
      .connect()
        .fromStream("sentence").to(new Normalization())
        .from(new Normalization()).to(new Analyze())
        .from(new Analyze()).to(new Update())
      .build();
    }
  }

  /**
   * Normalizes the sentences.
   */
  public static class Normalization extends AbstractFlowlet {
    
    /**
     * Emitter for emitting sentences from this Flowlet.
     */
    private OutputEmitter<String> out;
    
    @ProcessInput
    public void process(StreamEvent event) {
      String text = Bytes.toString(Bytes.toBytes(event.getBody()));
      if (text != null) {
        out.emit(text);
      }
    }
  }

  /**
   * Analyzes the sentences by passing the sentence to NLTK based sentiment analyzer
   * written in Python.
   */
//  public static class Analyze extends AbstractFlowlet {
//    
//    @Output("sentiments")
//    private OutputEmitter<String> sentiment;
//
//    @ProcessInput
//    public void process(String sentence) {
//      sentiment.emit(sentence);
//    }
//  }

  public static class Analyze extends ExternalProgramFlowlet<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Analyze.class);
    
    @Output("sentiments")
    private OutputEmitter<String> sentiment;
    
    private File workDir;
    
    /**
     * This method will be called at Flowlet initialization time.
     *
     * @param context The {@link com.continuuity.api.flow.flowlet.FlowletContext} for this Flowlet.
     * @return An {@link com.continuuity.flow.flowlet.ExternalProgramFlowlet.ExternalProgram}
     * to specify properties of the external program to process input.
     */
    @Override
    protected ExternalProgram init(FlowletContext context) {
      try {
        InputStream in = this.getClass().getClassLoader()
        .getResourceAsStream("sentiment-process.zip");
        
        if (in != null) {
          workDir = new File("work");
          Unzipper.unzip(in, workDir);
          
          File bash = new File("/bin/bash");
          
          if (!bash.exists()) {
            bash = new File("/usr/bin/bash");
          }
          if (bash.exists()) {
            File program = new File(workDir, "sentiment/score-sentence");
            return new ExternalProgram(bash, program.getAbsolutePath());
          }
        }
        throw new RuntimeException("Unable to start process");
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    
    /**
     * This method will be called for each input event to transform the given input
     * into string before sending to external program for processing.
     *
     * @param input The input event.
     * @return A UTF-8 encoded string of the input, or {@code null} if to skip this input.
     */
    @Override
    protected String encode(String input) {
      return input;
    }
    
    /**
     * This method will be called when the external program returns the result. Child
     * class can do its own processing in this method or could return an object of type
     * {@code OUT} for emitting to next Flowlet with the
     * {@link com.continuuity.api.flow.flowlet.OutputEmitter} returned by
     * {@link #getOutputEmitter()}.
     *
     * @param result The result from the external program.
     * @return The output to emit or {@code null} if nothing to emit.
     */
    @Override
    protected String processResult(String result) {
      return result;
    }
    
    /**
     * Child class can override this method to return an OutputEmitter for writing data
     * to the next Flowlet.
     *
     * @return An {@link com.continuuity.api.flow.flowlet.OutputEmitter} for type
     * {@code OUT}, or {@code null} if this flowlet doesn't have output.
     */
    @Override
    protected OutputEmitter<String> getOutputEmitter() {
      return sentiment;
    }
    
    @Override
    protected void finish() {
      try {
        LOG.info("Deleting work dir {}", workDir);
        FileUtils.deleteDirectory(workDir);
      } catch (IOException e) {
        LOG.error("Could not delete work dir {}", workDir);
        throw Throwables.propagate(e);
      }
    }
  }
  
  
  /**
   * Updates the timeseries table with sentiments received.
   */
  public static class Update extends AbstractFlowlet {
    
    @UseDataSet("sentiments")
    private Table sentiments;
    
    @UseDataSet("text-sentiments")
    private SimpleTimeseriesTable textSentiments;

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with()
      .setName("update")
      .setDescription("Updates the sentiment counts")
      .build();
    }
    
    @Batch(1)
    @ProcessInput("sentiments")
    public void process(Iterator<String> sentimentItr) {
      while (sentimentItr.hasNext()) {
        String text = sentimentItr.next();
        Iterable<String> parts = Splitter.on("---").split(text);
        if (Iterables.size(parts) == 2) {
          String sentence = Iterables.get(parts, 0);
          String sentiment = Iterables.get(parts, 1);
          sentiments.increment(new Increment("aggregate", sentiment, 1));
          textSentiments.write(new TimeseriesTable.Entry(sentiment.getBytes(Charsets.UTF_8),
                                                         sentence.getBytes(Charsets.UTF_8),
                                                         System.currentTimeMillis()));
        }
      }
    }
  }
  
  /**
   * Procedure that returns the aggregates timeseries sentiment data.
   */
  public static class SentimentAnalysisProcedure extends AbstractProcedure {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisProcedure.class);
    
    @UseDataSet("sentiments")
    private Table sentiments;
    
    @UseDataSet("text-sentiments")
    private SimpleTimeseriesTable textSentiments;
    
    @Handle("aggregates")
    public void sentimentAggregates(ProcedureRequest request, ProcedureResponder response)
    throws Exception {
      Row row = sentiments.get(new Get("aggregate"));
      Map<byte[], byte[]> result = row.getColumns();
      if (result == null) {
        response.error(ProcedureResponse.Code.FAILURE, "No sentiments processed.");
        return;
      }
      Map<String, Long> resp = Maps.newHashMap();
      for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
        resp.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
      }
      response.sendJson(ProcedureResponse.Code.SUCCESS, resp);
    }
    
    @Handle("sentiments")
    public void getSentiments(ProcedureRequest request, ProcedureResponder response)
    throws Exception {
      String sentiment = request.getArgument("sentiment");
      if (sentiment == null) {
        response.error(ProcedureResponse.Code.CLIENT_ERROR, "No sentiment sent");
        return;
      }
      
      long time = System.currentTimeMillis();
      List<SimpleTimeseriesTable.Entry> entries =
      textSentiments.read(sentiment.getBytes(Charsets.UTF_8),
                          time - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS),
                          time);
      
      Map<String, Long> textTimeMap = Maps.newHashMapWithExpectedSize(entries.size());
      for (SimpleTimeseriesTable.Entry entry : entries) {
        textTimeMap.put(Bytes.toString(entry.getValue()), entry.getTimestamp());
      }
      response.sendJson(ProcedureResponse.Code.SUCCESS, textTimeMap);
    }
    
    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
      .setName("sentiment-query")
      .setDescription("Sentiments Procedure")
      .withResources(ResourceSpecification.BASIC)
      .build();
    }
  }

//  /**
//   * A MapReduce job that aggregates results by classification.
//   */
//  public static class SentimentAnalysisMapReduce extends AbstractMapReduce {
//    // Annotation indicates the DataSet used in this MapReduce.
//    @UseDataSet("text-sentiments")
//    private SimpleTimeseriesTable textSentiments;
//    
//    @Override
//    public MapReduceSpecification configure() {
//      return MapReduceSpecification.Builder.with()
//      .setName("SentimentCountMapReduce")
//      .setDescription("Sentiment count MapReduce job")
//      // Specify the DataSet for Mapper to read.
//      .useInputDataSet("text-sentiments")
//      // Specify the DataSet for Reducer to write.
//      .useOutputDataSet("countTable")
//      .build();
//    }
//    
//    /**
//     * Define a MapReduce job.
//     * @param context the context of a MapReduce job
//     * @throws Exception
//     */
//    @Override
//    public void beforeSubmit(MapReduceContext context) throws Exception {
//      Job job = context.getHadoopJob();
//      long endTime = System.currentTimeMillis();
//      long startTime = endTime - TIME_WINDOW;
//      // A Mapper processes log data for the last 24 hours in logs table by 2 splits.
//      context.setInput(logs, logs.getInput(2, ROW_KEY, startTime, endTime));
//      // Set the Mapper class.
//      job.setMapperClass(LogMapper.class);
//      // Set the output key of the Reducer class.
//      job.setMapOutputKeyClass(LongWritable.class);
//      // Set the output value of the Reducer class.
//      job.setMapOutputValueClass(IntWritable.class);
//      // Set the Reducer class.
//      job.setReducerClass(LogReducer.class);
//    }
//    
//    /**
//     * A Mapper that transforms log data into key value pairs,
//     * where key is the timestamp on the hour scale and value
//     * the occurrence of a log. The Mapper receive a log in a
//     * key value pair (<byte[], TimeseriesTable.Entry>) from
//     * the input DataSet and outputs data in another key value pair
//     * (<LongWritable, IntWritable>) to the Reducer.
//     */
//    public static class LogMapper extends Mapper<byte[], TimeseriesTable.Entry, LongWritable, IntWritable> {
//      private static final long AGGREGATION_INTERVAL = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
//      // The output key and value.
//      private final LongWritable timestamp = new LongWritable();
//      private static final IntWritable ONE = new IntWritable(1);
//      
//      /**
//       * Generate key value pairs with key as timestamps by hour
//       * and value as occurrence of the log events.
//       * @param key the key of the input data. It doesn't contain valuable information for the Mapper.
//       * @param entry the value of the input data. An entry of the SimpleTimeSeriesTable contains
//       *              key, value, timestamp and tags.
//       * @param context the context of a MapReduce job
//       * @throws IOException
//       * @throws InterruptedException
//       */
//      @Override
//      public void map(byte[] key, TimeseriesTable.Entry entry, Context context)
//      throws IOException, InterruptedException {
//        // Convert timestamp to the hour scale.
//        long timestampByHour = entry.getTimestamp() - entry.getTimestamp() % AGGREGATION_INTERVAL;
//        timestamp.set(timestampByHour);
//        // Send the key value pair to Reducer.
//        context.write(timestamp, ONE);
//      }
//    }
//    
//    /**
//     * Aggregate the number of requests per hour and store the results in a SimpleTimeseriesTable.
//     */
//    public static class LogReducer extends Reducer<LongWritable, IntWritable, byte[], TimeseriesTable.Entry> {
//      /**
//       * Aggregate the number of requests by hour and store the results in the output DataSet.
//       * @param key the timestamp in hour
//       * @param values the occurrence of logs sent in one hour
//       * @param context the context of a MapReduce job
//       * @throws IOException
//       * @throws InterruptedException
//       */
//      public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
//      throws IOException, InterruptedException {
//        int count = 0;
//        // Get the count of logs sent in one hour.
//        for (IntWritable val : values) {
//          count += val.get();
//        }
//        // Store aggregated results in output DataSet.
//        // All aggregated results are stored in one row.
//        // Each result, the number of HTTP requests in the hour, is an entry of the row.
//        // The value of the entry is the number of the requests in the hour.
//        // The timestamp is the hour in milliseconds.
//        context.write(ROW_KEY, new TimeseriesTable.Entry(ROW_KEY, Bytes.toBytes(count), key.get()));
//      }
//    }
//  }

  
  
  
}
