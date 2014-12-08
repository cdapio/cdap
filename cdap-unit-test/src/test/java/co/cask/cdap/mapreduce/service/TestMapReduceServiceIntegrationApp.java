/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.mapreduce.service;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.test.app.MyKeyValueTableDefinition;
import com.google.common.io.ByteStreams;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * A dummy app with MapReduce program with service discovery for testing purpose
 */
public class TestMapReduceServiceIntegrationApp extends AbstractApplication {

  public static final String COUNT_METHOD_NAME = "count";
  public static final String INPUT_DATASET = "words";
  public static final String MR_NAME = "WordCountMR";
  public static final String OUTPUT_DATASET = "totals";
  public static final String SERVICE_NAME = "WordsCount";
  public static final String SERVICE_URL = "WordsCountServiceURL";
  public static final String SQUARE_METHOD_NAME = "square";
  public static final String SQUARED_TOTAL_WORDS_COUNT = "squared_total_words_count";

  @Override
  public void configure() {
    setName("MRServiceIntegration");
    addDatasetModule("my-kv", MyKeyValueTableDefinition.Module.class);
    createDataset(INPUT_DATASET, "myKeyValueTable", DatasetProperties.EMPTY);
    createDataset(OUTPUT_DATASET, "myKeyValueTable", DatasetProperties.EMPTY);
    addMapReduce(new CountTotal());
    addService(SERVICE_NAME, new WordsCountHandler());
  }

  /**
   * Map Reduce to count squared amount of all words in input dataset.
   */
  public static class CountTotal extends AbstractMapReduce {
    @Override
    public void configure() {
      setName(MR_NAME);
      setInputDataset(INPUT_DATASET);
      setOutputDataset(OUTPUT_DATASET);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(MyMapper.class);
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(LongWritable.class);
      job.setReducerClass(MyReducer.class);
      URL serviceURL = context.getServiceURL(SERVICE_NAME);
      job.getConfiguration().set(SERVICE_URL, serviceURL.toString());
    }

    private static URL getServiceUrl(TaskInputOutputContext context) throws MalformedURLException {
      Configuration configuration = context.getConfiguration();
      return new URL(configuration.get(SERVICE_URL));
    }

    /**
     * Mapper to count amount of words in sentence using service.
     */
    public static class MyMapper extends Mapper<String, String, BytesWritable, LongWritable> {

      private URL serviceUrl;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        serviceUrl = getServiceUrl(context);
      }

      @Override
      protected void map(String key, String value, Context context) throws IOException, InterruptedException {
        URL url = new URL(serviceUrl.toString() + COUNT_METHOD_NAME + "?words=" +
                            URLEncoder.encode(value, Charsets.UTF_8.name()));
        String wordCount = doRequest(url);
        context.write(new BytesWritable(Bytes.toBytes("total")), new LongWritable(Long.valueOf(wordCount)));
      }
    }

    /**
     * Reducer to count squared amount of words using service.
     */
    public static class MyReducer extends Reducer<BytesWritable, LongWritable, String, String> {

      private URL serviceUrl;

      @Override
      protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        serviceUrl = getServiceUrl(context);
      }

      @Override
      protected void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        long total = 0;
        for (LongWritable longWritable : values) {
          total += longWritable.get();
        }
        URL url = new URL(serviceUrl.toString() + SQUARE_METHOD_NAME + "?num=" + total);
        String squaredTotal = doRequest(url);
        context.write(SQUARED_TOTAL_WORDS_COUNT, squaredTotal);
      }
    }

    private static String doRequest(URL url) throws IOException {
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      String response;
      InputStream inputStream = null;
      try {
        inputStream = connection.getInputStream();
        response = new String(ByteStreams.toByteArray(inputStream), Charsets.UTF_8);
      } finally {
        if (inputStream != null) {
          inputStream.close();
        }
        connection.disconnect();
      }
      return response;
    }
  }

  public class WordsCountHandler extends AbstractHttpServiceHandler {

    @Path(COUNT_METHOD_NAME)
    @GET
    public void count(HttpServiceRequest request, HttpServiceResponder responder, @QueryParam("words") String words) {
      if (StringUtils.isEmpty(words)) {
        responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
      } else {
        responder.sendString(HttpURLConnection.HTTP_OK, Integer.toString(words.split(" ").length), Charsets.UTF_8);
      }
    }

    @Path(SQUARE_METHOD_NAME)
    @GET
    public void square(HttpServiceRequest request, HttpServiceResponder responder, @QueryParam("num") Long num) {
      if (num == null) {
        responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
      } else {
        responder.sendString(HttpURLConnection.HTTP_OK, Long.toString(num * num), Charsets.UTF_8);
      }
    }
  }
}
