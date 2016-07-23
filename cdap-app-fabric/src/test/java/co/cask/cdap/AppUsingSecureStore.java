/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * App used to test secure store apis
 * Includes: A service to access the secure store
 *           A spark program reading from the stream and writing to a dataset with (data, key)
 */
public class AppUsingSecureStore extends AbstractApplication {
  public static final String SERVICE_NAME = "secureStoreService";
  public static final String SPARK_NAME = "SparkSecureStoreProgram";
  public static final String STREAM_NAME = "testStream";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  private static final String NAMESPACE = "namespace";

  @Override
  public void configure() {
    setName("TestSecureStoreApis");
    setDescription("App to test usage of secure store apis");
    createDataset("result", KeyValueTable.class);
    addService(new SecureStoreService());
    addSpark(new SparkSecureStoreProgram());
  }

  public static class SecureStoreService extends AbstractService {
    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      addHandler(new SecureStoreHandler());
    }
  }

  public static class SecureStoreHandler extends AbstractHttpServiceHandler {
    private String namespace;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      namespace = context.getNamespace();
    }

    @Path("/put")
    @PUT
    public void put(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      byte[] value = new byte[request.getContent().remaining()];
      request.getContent().get(value);
      getContext().getAdmin().putSecureData(namespace, KEY, value, "", new HashMap<String, String>());
      responder.sendStatus(200);
    }

    @Path("/get")
    @GET
    public void get(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      try {
        byte[] bytes = getContext().getSecureData(namespace, KEY).get();
        responder.sendString(new String(bytes));
      } catch (IOException e) {
        responder.sendError(500, e.getMessage());
      }
    }

    @Path("/list")
    @GET
    public void list(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      String name = getContext().listSecureData(namespace).get(0).getName();
      responder.sendString(name);
    }

    @Path("/delete")
    @GET
    public void delete(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      getContext().getAdmin().deleteSecureData(namespace, KEY);
      responder.sendStatus(200);
    }
  }


  public static class SparkSecureStoreProgram extends AbstractSpark implements JavaSparkMain {
    @Override
    public void configure() {
      setName(SPARK_NAME);
      setDescription("Test Spark with Streams from other namespace");
      setMainClass(SparkSecureStoreProgram.class);
    }

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      final SecureStore secureStore = sec.getSecureStore();
      // Test secure store apis
      sec.getAdmin().putSecureData(NAMESPACE, KEY, VALUE.getBytes(), "", new HashMap<String, String>());
      Assert.assertEquals(new String(sec.getSecureData(NAMESPACE, KEY).get()), VALUE);
      Assert.assertEquals(new String(secureStore.getSecureData(NAMESPACE, KEY).get()), VALUE);
      sec.listSecureData(NAMESPACE);

      // Test access from a spark closure
      JavaSparkContext jsc = new JavaSparkContext();
      JavaPairRDD<Long, String> rdd = sec.fromStream(STREAM_NAME, String.class);
      JavaPairRDD<byte[], byte[]> resultRDD = rdd.mapToPair(new PairFunction<Tuple2<Long, String>,
        byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<Long, String> tuple2) throws Exception {
          return new Tuple2<>(Bytes.toBytes(tuple2._2()), secureStore.getSecureData(NAMESPACE, KEY).get());
        }
      });
      sec.saveAsDataset(resultRDD, "result");

      // Delete the key
      sec.getAdmin().deleteSecureData(NAMESPACE, KEY);
    }
  }
}
