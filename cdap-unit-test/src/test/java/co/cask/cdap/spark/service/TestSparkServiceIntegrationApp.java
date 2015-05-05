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

package co.cask.cdap.spark.service;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.io.Closeables;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.io.Charsets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A dummy app with spark program with service discovery for testing purpose
 */
public class TestSparkServiceIntegrationApp extends AbstractApplication {

  public static final String SERVICE_NAME = "SquareService";
  public static final String SERVICE_METHOD_NAME = "SquareService";

  @Override
  public void configure() {
    setName("TestSparkServiceIntegrationApp");
    setDescription("App to test Spark with Service");
    createDataset("result", KeyValueTable.class);
    addSpark(new SparkServiceProgramSpec());
    addService(SERVICE_NAME, new SquareHandler());
  }

  public static class SparkServiceProgramSpec extends AbstractSpark {
    @Override
    public void configure() {
      setName("SparkServiceProgram");
      setDescription("Test Spark with Service");
      setMainClass(SparkServiceProgram.class);
    }
  }

  public static class SparkServiceProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

      JavaRDD<Integer> distData = ((JavaSparkContext) context.getOriginalSparkContext()).parallelize(data);
      distData.count();
      final ServiceDiscoverer serviceDiscoverer = context.getServiceDiscoverer();
      JavaPairRDD<byte[], byte[]> resultRDD = distData.mapToPair(new PairFunction<Integer,
        byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Integer num) throws Exception {
          URL squareURL = serviceDiscoverer.getServiceURL(SERVICE_NAME);
          URLConnection connection = new URL(squareURL, String.format(SERVICE_METHOD_NAME + "/%s",
                                                                      String.valueOf(num))).openConnection();
          BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
          String squaredVale = reader.readLine();
          Closeables.closeQuietly(reader);
          return new Tuple2<byte[], byte[]>(Bytes.toBytes(String.valueOf(num)),
                                            Bytes.toBytes(squaredVale));
        }
      });
      context.writeToDataset(resultRDD, "result", byte[].class, byte[].class);
    }
  }

  public class SquareHandler extends AbstractHttpServiceHandler {
    @Path(SERVICE_METHOD_NAME + "/{num}")
    @GET
    public void square(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("num") String num) {
      if (num.isEmpty()) {
        responder.sendError(HttpResponseStatus.NO_CONTENT.code(), "No number provided");
      } else {
        responder.sendString(HttpResponseStatus.OK.code(), String.valueOf(Integer.parseInt(num) *
                                                                            Integer.parseInt(num)), Charsets.UTF_8);
      }
    }
  }
}
