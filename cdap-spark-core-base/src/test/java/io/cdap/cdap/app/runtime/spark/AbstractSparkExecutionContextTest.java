/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.api.app.AppStateStore;
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Unit tests for {@link AbstractSparkExecutionContext}.
 */
public class AbstractSparkExecutionContextTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * Dummy {@link AbstractSparkHttpServiceHandler} to exercise {@code SparkHttpServiceServer} start and stop.
   */
  public static final class DummySparkHttpHandler extends AbstractSparkHttpServiceHandler {
    @GET
    @Path("/ping")
    public String ping() {
      return "pong";
    }
  }

  @After
  public void resetSparkRuntimeEnvStopped() throws Exception {
    Field stoppedField = SparkRuntimeEnv$.MODULE$.getClass().getDeclaredField("stopped");
    stoppedField.setAccessible(true);
    stoppedField.setBoolean(SparkRuntimeEnv$.MODULE$, false);
  }

  @Test
  public void testDriverHttpServiceSparkHttpServiceServerAndOptionStatePaths() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    SparkHttpServiceHandlerSpecification handlerSpec = new SparkHttpServiceHandlerSpecification(
        DummySparkHttpHandler.class.getName(),
        Collections.<String, String>emptyMap(),
        Collections.<String>emptySet(),
        Collections.<ServiceHttpEndpoint>emptyList());

    final AtomicBoolean contextClosed = new AtomicBoolean(false);
    byte[] expectedState = new byte[]{1, 2, 3};
    AppStateStore appStateStore = Mockito.mock(AppStateStore.class);
    Mockito.when(appStateStore.getState("stateKey")).thenReturn(Optional.of(expectedState));
    Mockito.when(appStateStore.getState("missingKey")).thenReturn(Optional.empty());
    AppStateStoreProvider stateStoreProvider = Mockito.mock(AppStateStoreProvider.class);
    Mockito.when(stateStoreProvider.getStateStore(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(appStateStore);

    ProgramId programId = new ProgramId("default", "secApp", ProgramType.SPARK, "secSpark");
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        cConf, new Configuration(), programId, RunIds.generate(),
        Collections.singletonList(handlerSpec), null, new Closeable() {
          @Override
          public void close() {
            contextClosed.set(true);
          }
        }, Mockito.mock(LocationFactory.class), stateStoreProvider);

    SparkClassLoader sparkClassLoader = new SparkClassLoader(runtimeContext);
    DefaultSparkExecutionContext sec = new DefaultSparkExecutionContext(
        sparkClassLoader, Collections.<String, File>emptyMap());

    // 1. Verify sparkDriveHttpService was started via startAndWait() and published spark.repl.class.uri.
    String replUri = SparkRuntimeEnv.getProperty("spark.repl.class.uri");
    Assert.assertNotNull(replUri);
    HttpURLConnection driverConn = (HttpURLConnection) new URL(replUri + "/spark/stages/0/transaction")
        .openConnection();
    try {
      Assert.assertEquals(404, driverConn.getResponseCode());
    } finally {
      driverConn.disconnect();
    }

    // 2. Verify Optional / Scala Option methods on AbstractSparkExecutionContext.
    Assert.assertArrayEquals(expectedState, sec.getState("stateKey").orElse(null));
    Assert.assertFalse(sec.getState("missingKey").isPresent());
    Assert.assertTrue(sec.getWorkflowInfo().isEmpty());
    Assert.assertTrue(sec.getWorkflowToken().isEmpty());
    Assert.assertTrue(sec.getTriggeringScheduleInfo().isEmpty());

    // 3. Fire SparkListener onApplicationStart and onApplicationEnd to exercise SparkHttpServiceServer
    //    startAndWait() and stopAndWait().
    for (SparkListener listener : JavaConverters.seqAsJavaListConverter(SparkRuntimeEnv.getSparkListeners()).asJava()) {
      listener.onApplicationStart(new SparkListenerApplicationStart(
          "secApp", Option.apply("app-1"), 1000L, "user", Option.<String>empty(), Option.empty(), Option.empty()));
      listener.onApplicationEnd(new SparkListenerApplicationEnd(2000L));
    }
    sec.waitForSparkHttpService();

    // 4. close() stops sparkDriveHttpService via stopAndWait().
    sec.close();
    HttpURLConnection afterCloseConn = (HttpURLConnection) new URL(replUri + "/spark/stages/0/transaction")
        .openConnection();
    try {
      afterCloseConn.getResponseCode();
      Assert.fail("Expected IOException connecting to stopped SparkDriverHttpService");
    } catch (IOException expected) {
      // expected
    } finally {
      afterCloseConn.disconnect();
    }
  }
}
