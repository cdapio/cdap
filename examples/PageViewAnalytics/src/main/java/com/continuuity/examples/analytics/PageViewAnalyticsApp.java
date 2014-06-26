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
package com.continuuity.examples.analytics;

import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.HashPartition;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An Application that analyzes Apache access log events to find
 * the distribution of page views of a particular page.
 */
public class PageViewAnalyticsApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("PageViewAnalytics");
    setDescription("Page view analysis");
    addStream(new Stream("logEventStream"));
    createDataset("pageViewCDS", PageViewStore.class);
    addFlow(new LogAnalyticsFlow());
    addProcedure(new PageViewProcedure());
  }

  /**
   * Flows are used to perform real-time processing on Apache log events.
   * Flowlets are the building blocks of the Flow.
   * Flowlets are wired into a Directed Acyclic graph. 
   * LogAnalyticsFlow consists of two Flowlets:
   * - parser: a LogEventParseFlowlet that parses the referrer page URI
   *           and the requested page URI from a log event.
   * - pageCount: a PageCountFlowlet that increments the count of the
   *           requested page viewed from the referrer page.
   */
  public static class LogAnalyticsFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("PageViewFlow")
        .setDescription("Analyze page views")
        // Processing logic is written in Flowlets
        .withFlowlets()
          .add("parser", new LogEventParseFlowlet())
          .add("pageCount", new PageCountFlowlet())
        // Wire the Flowlets into a DAG
        .connect()
           // Data that is sent to the Stream is sent to the parser Flowlet
          .fromStream("logEventStream").to("parser")
           // After parsing, the data is sent to the pageCount Flowlet
          .from("parser").to("pageCount")
        .build();
    }
  }

  /**
   * Parse the referrer page URI and the requested page URI from a log event
   * into a PageView instance and emit it to the next Flowlet.
   */
  public static class LogEventParseFlowlet extends AbstractFlowlet {
    private static final Pattern ACCESS_LOG_PATTERN = Pattern.compile(
      //   IP       id    user      date          request     code     size    referrer    user agent
      "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
    
    // Emitter for emitting a PageView instance to the next Flowlet
    private OutputEmitter<PageView> output;
    
    // Declare a user-defined metric
    Metrics metrics;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void processFromStream(StreamEvent event) throws CharacterCodingException {
      
      // Get a log event in String format from a StreamEvent instance
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      Matcher matcher = ACCESS_LOG_PATTERN.matcher(log);
      if (matcher.matches() && matcher.groupCount() >= 8) {

        // Grab the referrer page URI from a log event
        String referrer = matcher.group(8);
        if (referrer.equals("-")) {
          
          // A Metric counts those log events without the referrer page URI field
          metrics.count("logs.noreferrer ", 1);
        } else {
          
          // Grab the requested page URI from the log event
          String request = matcher.group(5);
          int startIndex = request.indexOf(" ");
          int endIndex = request.indexOf(" ", startIndex + 1);
          String uri = request.substring(startIndex + 1, endIndex);

          // In order to use hash partition, specify the partitioning key in the emitter
          output.emit(new PageView(referrer, uri), "referrerHash", referrer.hashCode());
         }
      }
    }
  }

  /**
   * Aggregate the counts of the requested pages viewed from the referrer pages.
   */
  public static class PageCountFlowlet extends AbstractFlowlet {
    
    // UseDataSet annotation indicates the page-views DataSet is used in the Flowlet
    @UseDataSet("pageViewCDS")
    private PageViewStore pageViews;

    // Batch annotation indicates processing a batch of data objects to increase throughput
    @Batch(10)
    
    // HashPartition annotation indicates using hash partition to distribute data in multiple Flowlet instances
    @HashPartition("referrerHash")
    
    // ProcessInput annotation indicates that this method can process incoming data
    @ProcessInput
    public void count(Iterator<PageView> trackIterator) {
      while (trackIterator.hasNext()) {
        PageView pageView = trackIterator.next();

        // Increment the count of a PageView by 1
        pageViews.incrementCount(pageView);
      }
    }
  }
  
  /**
   * LogProcedure is used to serve processed log analysis results.
   */
  public static class PageViewProcedure extends AbstractProcedure {
    private static final Logger LOG = LoggerFactory.getLogger(PageViewProcedure.class);
    
    // Annotation indicates that the pageViewCDS custom DataSet is used in the Procedure
    @UseDataSet("pageViewCDS")
    private PageViewStore pageViews;

    /**
     * Get the distribution of requested pages viewed from a referrer page
     * @param request a ProcedureRequest sent by the user
     * @param responder a ProcedureResponder sent to the user
     * @throws IOException
     */
    @Handle("getDistribution")
    public void getPageDistribution(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      
      // A URI of the referrer page is passed by a runtime argument {"page": page-uri} in the request
      String referrer = request.getArgument("page");

      if (referrer == null) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "No page URI sent.");
        return;
      }

      // Get a map of a requested page URI to a count
      Map<String, Long> viewCount = pageViews.getPageCount(referrer);

      // Get the total number of requested pages viewed from the referrer page
      Long counts = pageViews.getCounts(referrer);

      if (viewCount == null || counts == 0) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "No page view processed.");
        return;
      }

      Map<String, Double> pageDistribution = new HashMap<String, Double>();

      for (Map.Entry<String, Long> entry : viewCount.entrySet()) {
        
        // Log the requested page URI and its count
        LOG.info("page URI:{}, count:{}", entry.getKey(), entry.getValue());
        
        // Create a map of the requested page URI to its distribution
        pageDistribution.put(entry.getKey(), entry.getValue() / (double) counts);
      }

      responder.sendJson(ProcedureResponse.Code.SUCCESS, pageDistribution);
    }
  }
}
