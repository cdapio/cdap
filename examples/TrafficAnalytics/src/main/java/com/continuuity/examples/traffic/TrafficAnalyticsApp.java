/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.traffic;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.lib.TimeseriesTable;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.google.common.base.Charsets;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TrafficAnalyticsApp analyzes Apache access log data and aggregates
 * the number of HTTP requests each hour over the last 24 hours.
 */
public class TrafficAnalyticsApp extends AbstractApplication {
  
  // The row key of TimeseriesTable.
  private static final byte[] ROW_KEY = Bytes.toBytes("f");
  
  // The time window of 1 day converted into milliseconds.
  private static final long TIME_WINDOW = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

  @Override
  public void configure() {
    setName("TrafficAnalytics");
    setDescription("HTTP request counts on an hourly basis");
    addStream(new Stream("logEventStream"));
    createDataset("logEventTable", TimeseriesTable.class, DatasetProperties.EMPTY);
    createDataset("countTable", TimeseriesTable.class, DatasetProperties.EMPTY);
    addFlow(new LogAnalyticsFlow());
    addProcedure(new LogCountProcedure());
    addMapReduce(new LogCountMapReduce());
  }

  /**
   * The Flow parses log data from the Stream and stores it in a TimeseriesTable.
  */
  public static class LogAnalyticsFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("RequestCountFlow")
        .setDescription("Read Apache access log event data and store in DataSet")
        .withFlowlets()
          .add("collector", new LogEventStoreFlowlet())
        .connect()
           // Log data sent to the Stream is sent to the collector Flowlet.
          .fromStream("logEventStream").to("collector")
        .build();
    }
  }

  /**
   * Parse time field of log data and store in a TimeseriesTable.
   */
  public static class LogEventStoreFlowlet extends AbstractFlowlet {
    
    // The regular expression pattern for parsing Apache access logs.
    private static final Pattern ACCESS_LOG_PATTERN = Pattern.compile(
                                                                      
    //   IP       id    user      date          request     code     size    referrer    user agent
    "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
    
    // The date format of log data.
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");

    // A Table to store the log data for MapReduce to process.
    @UseDataSet("logEventTable")
    private TimeseriesTable logs;

    // Annotation indicates that this method can process incoming data.
    @ProcessInput
    public void processFromStream(StreamEvent event) throws CharacterCodingException, ParseException {
      
      // Get a log in String format from a StreamEvent instance.
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      // Parse time fields of a log
      Matcher matcher = ACCESS_LOG_PATTERN.matcher(log);
      if (matcher.matches() && matcher.groupCount() >= 4) {
        String time = matcher.group(4);
        
        // Convert human readable time to Epoch time in millisecond.
        long timestamp = DATE_FORMAT.parse(time).getTime();
        
        // Store logs in one row. A log is kept in the value of an table entry.
        // MD5 hash code of the log is used as tag to distinguish logs with the same timestamp.
        logs.write(new TimeseriesTable.Entry(ROW_KEY, Bytes.toBytes(log), timestamp, MD5Hash.digest(log).getDigest()));
      }
    }
  }

  /**
   * A MapReduce job that aggregate log data by hour.
   */
  public static class LogCountMapReduce extends AbstractMapReduce {
    
    // Annotation indicates the DataSet used in this MapReduce.
    @UseDataSet("logEventTable")
    private TimeseriesTable logs;

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("RequestCountMapReduce")
        .setDescription("Apache access log count MapReduce job")
      
        // Specify the DataSet for Mapper to read.
        .useInputDataSet("logEventTable")
      
        // Specify the DataSet for Reducer to write.
        .useOutputDataSet("countTable")
        .build();
    }

    /**
     * Define a MapReduce job.
     *
     * @param context the context of a MapReduce job
     * @throws Exception
     */
    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      long endTime = System.currentTimeMillis();
      long startTime = endTime - TIME_WINDOW;
      
      // A Mapper processes log data for the last 24 hours in logs table by 2 splits.
      context.setInput(logs, logs.getInput(2, ROW_KEY, startTime, endTime));
      
      // Set the Mapper class.
      job.setMapperClass(LogMapper.class);
      
      // Set the output key of the Reducer class.
      job.setMapOutputKeyClass(LongWritable.class);
      
      // Set the output value of the Reducer class.
      job.setMapOutputValueClass(IntWritable.class);
      
      // Set the Reducer class.
      job.setReducerClass(LogReducer.class);
    }

    /**
     * A Mapper that transforms log data into key value pairs, 
     * where key is the timestamp on the hour scale and value
     * the occurrence of a log. The Mapper receive a log in a
     * key value pair (<byte[], TimeseriesTable.Entry>) from
     * the input DataSet and outputs data in another key value pair
     * (<LongWritable, IntWritable>) to the Reducer.
     */
    public static class LogMapper extends Mapper<byte[], TimeseriesTable.Entry, LongWritable, IntWritable> {
      private static final long AGGREGATION_INTERVAL = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
      // The output key and value.
      private final LongWritable timestamp = new LongWritable();
      private static final IntWritable ONE = new IntWritable(1);

      /**
       * Generate key value pairs with key as timestamps by hour
       * and value as occurrence of the log events.
       *
       * @param key the key of the input data. It doesn't contain valuable information for the Mapper.
       * @param entry the value of the input data. An entry of the SimpleTimeSeriesTable contains
       *              key, value, timestamp and tags.
       * @param context the context of a MapReduce job
       * @throws IOException
       * @throws InterruptedException
       */
      @Override
      public void map(byte[] key, TimeseriesTable.Entry entry, Context context)
        throws IOException, InterruptedException {
          
        // Convert timestamp to the hour scale.
        long timestampByHour = entry.getTimestamp() - entry.getTimestamp() % AGGREGATION_INTERVAL;
        timestamp.set(timestampByHour);
          
        // Send the key value pair to Reducer.
        context.write(timestamp, ONE);
      }
    }

    /**
     * Aggregate the number of requests per hour and store the results in a TimeseriesTable.
     */
    public static class LogReducer extends Reducer<LongWritable, IntWritable, byte[], TimeseriesTable.Entry> {
      /**
       * Aggregate the number of requests by hour and store the results in the output DataSet.
       *
       * @param key the timestamp in hour
       * @param values the occurrence of logs sent in one hour
       * @param context the context of a MapReduce job
       * @throws IOException
       * @throws InterruptedException
       */
      public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int count = 0;
          
        // Get the count of logs sent in one hour.
        for (IntWritable val : values) {
          count += val.get();
        }
        // Store aggregated results in output DataSet.
        // All aggregated results are stored in one row.
        // Each result, the number of HTTP requests in the hour, is an entry of the row.
        // The value of the entry is the number of the requests in the hour.
        // The timestamp is the hour in milliseconds.
        context.write(ROW_KEY, new TimeseriesTable.Entry(ROW_KEY, Bytes.toBytes(count), key.get()));
      }
    }
  }

  /**
   * LogCountProcedure is used to query the number of requests hourly
   * in a time range, by default, of the last 24 hours.
   */
  public static class LogCountProcedure extends AbstractProcedure {
    // Annotation indicates that countTable dataset is used in the procedure.
    @UseDataSet("countTable")
    private TimeseriesTable countTable;

    @Handle("getCounts")
    public void getAllCounts(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException{
        
      // Get the end time of the time range from the query parameters. By default, end time is now.
      String endTs = request.getArgument("endTs");
      long endTime = (endTs == null) ? System.currentTimeMillis() : Long.parseLong(endTs);
        
      // Get the start time of the time range from the query parameters.
      // By default, start time is 24 hours ago from now.
      String startTs = request.getArgument("startTs");
      long startTime = (startTs == null) ? endTime - TIME_WINDOW : Long.parseLong(startTs);

      Map<Long, Integer> hourCount = new HashMap<Long, Integer>();

      for (TimeseriesTable.Entry entry : countTable.read(ROW_KEY, startTime, endTime)) {
        hourCount.put(entry.getTimestamp(), Bytes.toInt(entry.getValue()));
      }

      // Send response with JSON format.
      responder.sendJson(hourCount);
    }
  }
}
