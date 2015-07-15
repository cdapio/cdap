/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.loganalysis;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.Workflow;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Application that demonstrate running a Spark and MapReduce simultaneously in a {@link Workflow}
 */
public class LogAnalysisApp extends AbstractApplication {

  public static final String LOG_STREAM = "logStream";
  public static final String HIT_COUNTER_SERVICE = "HitCounterService";
  public static final String RESPONSE_COUNTER_SERVICE = "ResponseCounterService";
  public static final String RESPONSE_COUNT_STORE = "responseCount";
  public static final String HIT_COUNT_STORE = "hitCount";
  public static final String IP_COUNT_STORE = "ipCount";

  @Override
  public void configure() {
    setDescription("CDAP Log Analysis App");

    // A stream to ingest log data
    addStream(new Stream(LOG_STREAM));

    // A Spark and MapReduce for processing log data
    addSpark(new ResponseCounterSpark());
    addMapReduce(new HitCounterProgram());

    addWorkflow(new LogAnalysisWorkflow());

    // Services to query for result
    addService(HIT_COUNTER_SERVICE, new HitCounterServiceHandler());
    addService(RESPONSE_COUNTER_SERVICE, new ResponseCounterHandler());

    // Datasets to store output after processing
    createDataset(RESPONSE_COUNT_STORE, KeyValueTable.class);
    createDataset(HIT_COUNT_STORE, KeyValueTable.class);
    createDataset(IP_COUNT_STORE, TimePartitionedFileSet.class, FileSetProperties.builder()
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
  }

  /**
   * A Workflow which ties spark and mapreduce program together for log analysis
   */
  public static class LogAnalysisWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setDescription("Runs log analysis spark and mapreduce programs simultaneously");
      fork()
        .addMapReduce(HitCounterProgram.class.getSimpleName())
        .also()
        .addSpark(ResponseCounterSpark.class.getSimpleName())
        .join();
    }
  }

  /**
   * Specification for the Spark program in this application
   */
  public static final class ResponseCounterSpark extends AbstractSpark {

    @Override
    public void configure() {
      setDescription("Counts the total number of responses for every unique response code");
      setMainClass(ResponseCounterProgram.class);
    }
  }

  /**
   * A {@link Service} that responds with total number of hits for a given URL or path
   */
  public static final class HitCounterServiceHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();
    private static final String URL_KEY = "url";
    static final String HIT_COUNTER_SERVICE_PATH = "hitcount";

    @UseDataSet(HIT_COUNT_STORE)
    private KeyValueTable hitCountStore;

    @Path(HIT_COUNTER_SERVICE_PATH)
    @POST
    public void getHitCount(HttpServiceRequest request, HttpServiceResponder responder) {
      String urlRequest = Charsets.UTF_8.decode(request.getContent()).toString();
      String url = GSON.fromJson(urlRequest, JsonObject.class).get(URL_KEY).getAsString();
      if (url == null) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                             "A url or path must be specified with \"url\" as key in JSON.",
                             Charsets.UTF_8);
        return;
      }

      // Get the total number of hits from the dataset for this path
      byte[] hitCount = hitCountStore.read(url.getBytes(Charsets.UTF_8));
      if (hitCount == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No record found of %s", url), Charsets.UTF_8);
      } else {
        responder.sendString(String.valueOf(Bytes.toLong(hitCount)));
      }
    }
  }

  /**
   * A {@link Service} that responds with total number of responses for a given response code
   */
  public static final class ResponseCounterHandler extends AbstractHttpServiceHandler {

    static final String RESPONSE_COUNT_PATH = "rescount";

    @UseDataSet(RESPONSE_COUNT_STORE)
    private KeyValueTable responseCountstore;

    @Path(RESPONSE_COUNT_PATH + "/{rescode}")
    @GET
    public void centers(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("rescode") Integer responseCode) {

      byte[] read = responseCountstore.read(Bytes.toBytes(responseCode));
      if (read == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No record found for response code: %s", responseCode), Charsets.UTF_8);
      } else {
        responder.sendString(String.valueOf(Bytes.toLong(read)));
      }
    }
  }
}
