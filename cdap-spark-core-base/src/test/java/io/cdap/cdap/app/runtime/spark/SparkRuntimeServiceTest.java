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

import com.google.common.util.concurrent.Service;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.app.runtime.spark.submit.SparkJobFuture;
import io.cdap.cdap.app.runtime.spark.submit.SparkSubmitter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SparkRuntimeService}.
 */
public class SparkRuntimeServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @After
  public void cleanUpPy4jFiles() throws IOException {
    Files.deleteIfExists(new File(SparkRuntimeUtils.PYSPARK_PORT_FILE_NAME).toPath());
    Files.deleteIfExists(new File(SparkRuntimeUtils.PYSPARK_SECRET_FILE_NAME).toPath());
  }

  /**
   * Custom Hadoop {@link FileSystem} with scheme {@code testrawfs} to exercise the FileSystem copy branch
   * in {@code SparkRuntimeService.getPySparkScript}.
   */
  public static final class TestRawFileSystem extends RawLocalFileSystem {
    @Override
    public URI getUri() {
      return URI.create("testrawfs:///");
    }
  }

  @Test
  public void testServiceNameArtifactFetcherStopTimeoutAndPySparkFileSystemCopy() throws Exception {
    File localDataDir = TEMP_FOLDER.newFolder();
    File artifactsDir = new File("./artifacts").getAbsoluteFile();
    boolean createdArtifactsDir = !artifactsDir.exists() && artifactsDir.mkdirs();

    try {
      CConfiguration cConf = CConfiguration.create();
      cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
      cConf.setInt(Constant.Spark.ArtifactFetcher.PORT, 0);

      Configuration hConf = new Configuration();
      hConf.setClass("fs.testrawfs.impl", TestRawFileSystem.class, FileSystem.class);

      File sourcePy = new File(TEMP_FOLDER.newFolder(), "script_source.txt");
      final String expectedPyContent = "print('copied via hadoop filesystem')\n";
      Files.write(sourcePy.toPath(), expectedPyContent.getBytes(StandardCharsets.UTF_8));
      final URI customSchemeUri = URI.create("testrawfs://" + sourcePy.getAbsolutePath());

      AbstractSpark pySpark = new AbstractSpark() {
        @Override
        protected void configure() {
        }

        @Override
        public void initialize() {
          SparkClientContext ctx = getContext();
          ctx.setPySparkScript(customSchemeUri);
        }
      };

      LocationFactory mockLocationFactory = createNonLocalHomeLocationFactory(TEMP_FOLDER.newFolder());
      SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(cConf, hConf);

      final AtomicReference<String> copiedContent = new AtomicReference<>();
      final CountDownLatch submittedLatch = new CountDownLatch(1);

      SparkSubmitter blockingSubmitter = new SparkSubmitter() {
        @Override
        public <V> SparkJobFuture<V> submit(
            SparkRuntimeContext context, Map<String, String> configs,
            List<LocalizeResource> resources, URI jobFile, final V result) throws Exception {
          copiedContent.set(new String(Files.readAllBytes(new File(jobFile).toPath()), StandardCharsets.UTF_8));
          ControllableSparkJobFuture<V> jobFuture = new ControllableSparkJobFuture<>();
          submittedLatch.countDown();
          return jobFuture;
        }
      };

      MockMasterEnvironment masterEnv = new MockMasterEnvironment();
      masterEnv.initialize(null);
      CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
          cConf, new NoOpMetricsCollectionService(), auditLogContexts -> { });

      final SparkRuntimeService service = new SparkRuntimeService(
          cConf, pySpark, null, null, runtimeContext, blockingSubmitter,
          mockLocationFactory, false, Mockito.mock(FieldLineageWriter.class), masterEnv, nettyFactory);

      Assert.assertEquals("Spark - testSpark", service.serviceName());

      service.startAsync().awaitRunning();
      Assert.assertTrue(submittedLatch.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(expectedPyContent, copiedContent.get());

      // Exercise stop(timeout, unit) without capturing return type so it compiles across Guava 13 and 30.
      service.stop(2, TimeUnit.SECONDS);
      Tasks.waitFor(Service.State.TERMINATED, new Callable<Service.State>() {
        @Override
        public Service.State call() {
          return service.state();
        }
      }, 5, TimeUnit.SECONDS);
    } finally {
      if (createdArtifactsDir) {
        artifactsDir.delete();
      }
    }
  }

  @Test
  public void testPySparkScriptUrlOpenStreamFallbackCopy() throws Exception {
    final String expectedHttpScript = "print('copied via http url fallback')\n";
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.createContext("/job.py", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        byte[] bytes = expectedHttpScript.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(bytes);
        }
      }
    });
    httpServer.start();

    try {
      final URI httpScriptUri = URI.create(
          "http://127.0.0.1:" + httpServer.getAddress().getPort() + "/job.py");

      CConfiguration cConf = CConfiguration.create();
      cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
      cConf.setBoolean(Constants.Environment.PROGRAM_SUBMISSION_MASTER_ENV_ENABLED, false);

      AbstractSpark pySpark = new AbstractSpark() {
        @Override
        protected void configure() {
        }

        @Override
        public void initialize() {
          getContext().setPySparkScript(httpScriptUri);
        }
      };

      LocationFactory mockLocationFactory = createNonLocalHomeLocationFactory(TEMP_FOLDER.newFolder());
      SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(cConf, new Configuration());

      final AtomicReference<String> copiedScript = new AtomicReference<>();
      SparkSubmitter immediateSubmitter = new SparkSubmitter() {
        @Override
        public <V> SparkJobFuture<V> submit(
            SparkRuntimeContext context, Map<String, String> configs,
            List<LocalizeResource> resources, URI jobFile, V result) throws Exception {
          copiedScript.set(new String(Files.readAllBytes(new File(jobFile).toPath()), StandardCharsets.UTF_8));
          ControllableSparkJobFuture<V> future = new ControllableSparkJobFuture<>();
          future.complete(result);
          return future;
        }
      };

      MockMasterEnvironment masterEnv = new MockMasterEnvironment();
      masterEnv.initialize(null);
      CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
          cConf, new NoOpMetricsCollectionService(), auditLogContexts -> { });

      final SparkRuntimeService service = new SparkRuntimeService(
          cConf, pySpark, null, null, runtimeContext, immediateSubmitter,
          mockLocationFactory, false, Mockito.mock(FieldLineageWriter.class), masterEnv, nettyFactory);

      service.startAsync().awaitRunning();
      Tasks.waitFor(Service.State.TERMINATED, new Callable<Service.State>() {
        @Override
        public Service.State call() {
          return service.state();
        }
      }, 5, TimeUnit.SECONDS);

      Assert.assertEquals(expectedHttpScript, copiedScript.get());
    } finally {
      httpServer.stop(0);
    }
  }

  private static final class ControllableSparkJobFuture<V> implements SparkJobFuture<V> {
    private final CompletableFuture<V> delegate = new CompletableFuture<>();

    void complete(V value) {
      delegate.complete(value);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean cancel(long gracefulTimeout, TimeUnit gracefulTimeoutUnit) {
      return delegate.cancel(true);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return delegate.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.get(timeout, unit);
    }
  }

  private static LocationFactory createNonLocalHomeLocationFactory(File backingDir) {
    final LocalLocationFactory delegate = new LocalLocationFactory(backingDir);
    Location fakeHome = Mockito.mock(Location.class);
    Mockito.when(fakeHome.toURI()).thenReturn(URI.create("mem://cluster/home"));

    LocationFactory factory = Mockito.mock(LocationFactory.class);
    Mockito.when(factory.getHomeLocation()).thenReturn(fakeHome);
    Mockito.when(factory.create(Mockito.anyString())).thenAnswer(invocation ->
        delegate.create((String) invocation.getArguments()[0]));
    Mockito.when(factory.create(Mockito.any(URI.class))).thenAnswer(invocation ->
        delegate.create((URI) invocation.getArguments()[0]));
    return factory;
  }
}
