/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpContentConsumer;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * A spark program with {@link SparkHttpServiceHandler}.
 */
public class SparkServiceProgram extends AbstractExtendedSpark implements JavaSparkMain {

  @Override
  protected void configure() {
    setMainClass(SparkServiceProgram.class);
    addHandlers(new TestSparkHandler());
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
  }

  /**
   * A {@link SparkHttpServiceHandler} for testing.
   */
  public static final class TestSparkHandler extends AbstractSparkHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TestSparkHandler.class);

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      try {
        context.getAdmin().createDataset("wordcount", FileSet.class.getName(), FileSetProperties.builder()
          .setInputFormat(TextInputFormat.class)
          .build());
      } catch (InstanceConflictException e) {
        // It's ok if the dataset already exists
      }
    }

    @GET
    @Path("/sum")
    public void sum(HttpServiceRequest request, HttpServiceResponder responder,
                    @QueryParam("n") List<Integer> numbers) {
      // Sum n numbers from the query param
      JavaSparkContext jsc = getContext().getJavaSparkContext();
      Integer result = jsc.parallelize(numbers).reduce((v1, v2) -> v1 + v2);
      responder.sendString(result.toString());
    }

    @POST
    @Path("/wordcount")
    public SparkHttpContentConsumer wordcount(HttpServiceRequest request,
                                              HttpServiceResponder responder) throws IOException {
      Location tmpLocation = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset("wordcount").getLocation(UUID.randomUUID().toString());
      });

      WritableByteChannel outputChannel = Channels.newChannel(tmpLocation.getOutputStream());
      return new SparkHttpContentConsumer() {
        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          outputChannel.write(chunk);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          try {
            Map<String, Integer> result = getContext().getJavaSparkContext().textFile(tmpLocation.toURI().toString())
              .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split("\\s+")))
              .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
              .reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2)
              .collectAsMap();
            responder.sendJson(200, result, new TypeToken<Map<String, Integer>>() { }.getType(), new Gson());
          } finally {
            tmpLocation.delete();
          }
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          try {
            tmpLocation.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete temporary location {}", tmpLocation, e);
          }
        }
      };
    }
  }
}
