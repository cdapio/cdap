/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.test.NoopAdmin;
import co.cask.cdap.internal.app.preview.NoopDataTracerFactory;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class HttpHandlerGeneratorTest {

  private static final String IN_TX = "in-tx";

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Path("/p1")
  public abstract static class BaseHttpHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/handle")
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void process(HttpServiceRequest request, HttpServiceResponder responder) {
      Assert.assertNull(System.getProperty(IN_TX));
      responder.sendString("Hello World");
    }
  }

  @Path("/p2")
  public static final class MyHttpHandler extends BaseHttpHandler {

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
    }

    @Path("/echo/{name}")
    @POST
    public void echo(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("name") String name) {
      Assert.assertNotNull(System.getProperty(IN_TX));
      responder.sendString(Charsets.UTF_8.decode(request.getContent()).toString() + " " + name);
    }

    @Path("/echo/firstHeaders")
    @GET
    public void echoFirstHeaders(HttpServiceRequest request, HttpServiceResponder responder) {
      Assert.assertNotNull(System.getProperty(IN_TX));
      Map<String, List<String>> headers = request.getAllHeaders();
      responder.sendStatus(200, Maps.transformValues(headers, new Function<List<String>, String>() {
        @Override
        public String apply(List<String> input) {
          return input.iterator().next();
        }
      }));
    }

    @Path("/echo/allHeaders")
    @GET
    public void echoAllHeaders(HttpServiceRequest request, HttpServiceResponder responder) {
      Assert.assertNotNull(System.getProperty(IN_TX));
      List<Map.Entry<String, String>> headers = new ArrayList<>();
      for (Map.Entry<String, List<String>> entry : request.getAllHeaders().entrySet()) {
        for (String value : entry.getValue()) {
          headers.add(Maps.immutableEntry(entry.getKey(), value));
        }
      }

      responder.sendStatus(200, headers);
    }
  }

  // Omit class-level PATH annotation, to verify that prefix is still prepended to handled path.
  public static final class NoAnnotationHandler extends AbstractHttpServiceHandler {

    @Path("/ping")
    @GET
    public void echo(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString("OK");
    }
  }

  /**
   * A testing handler for testing file upload and download through usage of {@link HttpContentConsumer}
   * and {@link HttpContentProducer}.
   */
  public static final class FileHandler extends AbstractHttpServiceHandler {

    private final File outputDir;

    public FileHandler(File outputDir) {
      this.outputDir = outputDir;
    }

    @Path("/upload/{file}")
    @PUT
    public HttpContentConsumer upload(HttpServiceRequest request,
                                      HttpServiceResponder responder,
                                      @PathParam("file") String file) throws IOException {
      Assert.assertNotNull(System.getProperty(IN_TX));
      return new FileContentConsumer(new File(outputDir, file));
    }

    @Path("/upload-no-tx/{file}")
    @PUT
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public HttpContentConsumer uploadNoTx(HttpServiceRequest request,
                                          HttpServiceResponder responder,
                                          @PathParam("file") String file) throws IOException {
      Assert.assertNull(System.getProperty(IN_TX));
      return new NoTxFileContentConsumer(new File(outputDir, file));
    }

    @Path("/download/{file}")
    @GET
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void download(HttpServiceRequest request,
                         HttpServiceResponder responder,
                         @PathParam("file") String file) {
      Assert.assertNull(System.getProperty(IN_TX));
      Location location = Locations.toLocation(new File(outputDir, file));
      try {
        responder.send(200, location, "text/plain");
      } catch (IOException e) {
        responder.sendStatus(500);
      }
    }

    // A POST endpoint for upload file and response with the file content using HttpContentProducer
    @Path("/upload/{file}")
    @POST
    public HttpContentConsumer uploadDownload(HttpServiceRequest request,
                                              HttpServiceResponder responder,
                                              @PathParam("file") String file) throws IOException {
      Assert.assertNotNull(System.getProperty(IN_TX));
      final File targetFile = new File(outputDir, file);
      return new FileContentConsumer(targetFile) {
        @Override
        protected void response(HttpServiceResponder responder) throws IOException {
          responder.send(200, Locations.toLocation(targetFile), "text/plain");
        }
      };
    }
  }

  /**
   * A {@link HttpContentConsumer} that writes uploaded bytes to a file.
   */
  private static class FileContentConsumer extends HttpContentConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(FileContentConsumer.class);
    private final WritableByteChannel channel;

    FileContentConsumer(File file) throws IOException {
      this.channel = new FileOutputStream(file).getChannel();
    }

    @Override
    public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
      channel.write(chunk);
    }

    @Override
    public void onFinish(HttpServiceResponder responder) throws Exception {
      validateTransaction();
      channel.close();
      response(responder);
    }

    @Override
    public void onError(HttpServiceResponder responder, Throwable failureCause) {
      validateTransaction();
      Closeables.closeQuietly(channel);
      LOG.error("Failed when handling upload", failureCause);
    }

    protected void response(HttpServiceResponder responder) throws IOException {
      responder.sendStatus(200);
    }

    protected void validateTransaction() {
      Assert.assertNotNull(System.getProperty(IN_TX));
    }
  }

  private static class NoTxFileContentConsumer extends FileContentConsumer {

    NoTxFileContentConsumer(File file) throws IOException {
      super(file);
    }

    @Override
    public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
      super.onReceived(chunk, transactional);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void onFinish(HttpServiceResponder responder) throws Exception {
      super.onFinish(responder);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void onError(HttpServiceResponder responder, Throwable failureCause) {
      super.onError(responder, failureCause);
    }

    @Override
    protected void validateTransaction() {
      Assert.assertNull(System.getProperty(IN_TX));
    }
  }

  @Test
  public void testHttpHeaders() throws Exception {
    MetricsContext noOpsMetricsContext =
      new NoOpMetricsCollectionService().getContext(new HashMap<String, String>());
    HttpHandlerFactory factory = new HttpHandlerFactory("/prefix", noOpsMetricsContext);

    HttpHandler httpHandler = factory.createHttpHandler(
      TypeToken.of(MyHttpHandler.class), new AbstractDelegatorContext<MyHttpHandler>() {
        @Override
        protected MyHttpHandler createHandler() {
          return new MyHttpHandler();
        }
      });

    NettyHttpService service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(httpHandler))
      .build();

    service.startAndWait();
    try {
      InetSocketAddress bindAddress = service.getBindAddress();

      // Make a request with headers that the response should carry first value for each header name
      HttpURLConnection urlConn = (HttpURLConnection) new URL(String.format("http://%s:%d/prefix/p2/echo/firstHeaders",
                                                                            bindAddress.getHostName(),
                                                                            bindAddress.getPort())).openConnection();
      urlConn.addRequestProperty("k1", "v1");
      urlConn.addRequestProperty("k1", "v2");
      urlConn.addRequestProperty("k2", "v2");

      Assert.assertEquals(200, urlConn.getResponseCode());
      Map<String, List<String>> headers = urlConn.getHeaderFields();
      Assert.assertEquals(ImmutableList.of("v1"), headers.get("k1"));
      Assert.assertEquals(ImmutableList.of("v2"), headers.get("k2"));

      // Make a request with headers that the response should carry all values for each header name
      urlConn = (HttpURLConnection) new URL(String.format("http://%s:%d/prefix/p2/echo/allHeaders",
                                                          bindAddress.getHostName(),
                                                          bindAddress.getPort())).openConnection();
      urlConn.addRequestProperty("k1", "v1");
      urlConn.addRequestProperty("k1", "v2");
      urlConn.addRequestProperty("k1", "v3");
      urlConn.addRequestProperty("k2", "v2");

      Assert.assertEquals(200, urlConn.getResponseCode());
      headers = urlConn.getHeaderFields();
      // URLConnection always reverse the ordering of the header values.
      Assert.assertEquals(ImmutableList.of("v3", "v2", "v1"), headers.get("k1"));
      Assert.assertEquals(ImmutableList.of("v2"), headers.get("k2"));
    } finally {
      service.stopAndWait();
    }
  }

  @Test
  public void testContentConsumer() throws Exception {
    MetricsContext noOpsMetricsContext =
      new NoOpMetricsCollectionService().getContext(new HashMap<String, String>());
    HttpHandlerFactory factory = new HttpHandlerFactory("/content", noOpsMetricsContext);

    // Create the file upload handler and starts a netty server with it
    final File outputDir = TEMP_FOLDER.newFolder();
    HttpHandler httpHandler = factory.createHttpHandler(
      TypeToken.of(FileHandler.class), new AbstractDelegatorContext<FileHandler>() {
        @Override
        protected FileHandler createHandler() {
          return new FileHandler(outputDir);
        }
      });

    // Creates a Netty http server with 1K request buffer
    NettyHttpService service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(httpHandler))
      .setHttpChunkLimit(1024)
      .build();

    service.startAndWait();
    try {
      InetSocketAddress bindAddress = service.getBindAddress();
      testUpload(outputDir, bindAddress, "");
      testUpload(outputDir, bindAddress, "-no-tx");
    } finally {
      service.stopAndWait();
    }
  }

  private void testUpload(File outputDir, InetSocketAddress bindAddress, String suffix) throws Exception {
    // Make a PUT call
    HttpURLConnection urlConn = (HttpURLConnection) new URL(
      String.format("http://%s:%d/content/upload%s/test%s.txt",
                    bindAddress.getHostName(), bindAddress.getPort(), suffix, suffix)).openConnection();
    try {
      urlConn.setReadTimeout(2000);
      urlConn.setDoOutput(true);
      urlConn.setRequestMethod("PUT");
      // Set to use default chunk size
      urlConn.setChunkedStreamingMode(-1);

      // Write over 1MB of data
      // One fragment is 10K of data
      byte[] fragment = Strings.repeat("0123456789", 1024).getBytes(Charsets.UTF_8);
      File localFile = TEMP_FOLDER.newFile();
      try (
        OutputStream os = urlConn.getOutputStream();
        FileOutputStream fos = new FileOutputStream(localFile)
      ) {
        for (int i = 0; i < 100; i++) {
          os.write(fragment);
          fos.write(fragment);
        }
      }

      Assert.assertEquals(200, urlConn.getResponseCode());

      // File written on the remote end should be > 1K and should be the same as the local one
      File remoteFile = new File(outputDir, String.format("test%s.txt", suffix));
      Assert.assertTrue(remoteFile.length() > 1024);
      Assert.assertEquals(Files.hash(localFile, Hashing.md5()), Files.hash(remoteFile, Hashing.md5()));
    } finally {
      urlConn.disconnect();
    }
  }

  @Test
  public void testContentProducer() throws Exception {
    MetricsContext noOpsMetricsContext =
      new NoOpMetricsCollectionService().getContext(new HashMap<String, String>());
    HttpHandlerFactory factory = new HttpHandlerFactory("/content", noOpsMetricsContext);

    // Create the file upload handler and starts a netty server with it
    final File outputDir = TEMP_FOLDER.newFolder();
    HttpHandler httpHandler = factory.createHttpHandler(
      TypeToken.of(FileHandler.class), new AbstractDelegatorContext<FileHandler>() {
        @Override
        protected FileHandler createHandler() {
          return new FileHandler(outputDir);
        }
      });

    NettyHttpService service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(httpHandler))
      .build();

    service.startAndWait();
    try {
      // Generate a 100K file
      File file = TEMP_FOLDER.newFile();
      Files.write(Strings.repeat("0123456789", 10240).getBytes(Charsets.UTF_8), file);

      InetSocketAddress bindAddress = service.getBindAddress();

      // Upload the generated file
      URL uploadURL = new URL(String.format("http://%s:%d/content/upload/test.txt",
                                            bindAddress.getHostName(), bindAddress.getPort()));
      HttpURLConnection urlConn = (HttpURLConnection) uploadURL.openConnection();
      try {
        urlConn.setDoOutput(true);
        urlConn.setRequestMethod("PUT");
        Files.copy(file, urlConn.getOutputStream());
        Assert.assertEquals(200, urlConn.getResponseCode());
      } finally {
        urlConn.disconnect();
      }

      // Download the file
      File downloadFile = TEMP_FOLDER.newFile();
      urlConn = (HttpURLConnection) new URL(
        String.format("http://%s:%d/content/download/test.txt",
                      bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      try {
        ByteStreams.copy(urlConn.getInputStream(), Files.newOutputStreamSupplier(downloadFile));
      } finally {
        urlConn.disconnect();
      }

      // Compare if the file content are the same
      Assert.assertTrue(Files.equal(file, downloadFile));

      // Download a file that doesn't exist
      urlConn = (HttpURLConnection) new URL(
        String.format("http://%s:%d/content/download/test2.txt",
                      bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      try {
        Assert.assertEquals(500, urlConn.getResponseCode());
      } finally {
        urlConn.disconnect();
      }

      // Upload the file to the POST endpoint. The endpoint should response with the same file content
      downloadFile = TEMP_FOLDER.newFile();
      urlConn = (HttpURLConnection) uploadURL.openConnection();
      try {
        urlConn.setDoOutput(true);
        urlConn.setRequestMethod("POST");
        Files.copy(file, urlConn.getOutputStream());
        ByteStreams.copy(urlConn.getInputStream(), Files.newOutputStreamSupplier(downloadFile));
        Assert.assertEquals(200, urlConn.getResponseCode());
        Assert.assertTrue(Files.equal(file, downloadFile));
      } finally {
        urlConn.disconnect();
      }

    } finally {
      service.stopAndWait();
    }
  }

  @Test
  public void testHttpHandlerGenerator() throws Exception {
    MetricsContext noOpsMetricsContext =
      new NoOpMetricsCollectionService().getContext(new HashMap<String, String>());
    HttpHandlerFactory factory = new HttpHandlerFactory("/prefix", noOpsMetricsContext);

    HttpHandler httpHandler = factory.createHttpHandler(
      TypeToken.of(MyHttpHandler.class), new AbstractDelegatorContext<MyHttpHandler>() {
      @Override
      protected MyHttpHandler createHandler() {
        return new MyHttpHandler();
      }
    });

    HttpHandler httpHandlerWithoutAnnotation = factory.createHttpHandler(
      TypeToken.of(NoAnnotationHandler.class), new AbstractDelegatorContext<NoAnnotationHandler>() {
      @Override
      protected NoAnnotationHandler createHandler() {
        return new NoAnnotationHandler();
      }
    });

    NettyHttpService service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(httpHandler, httpHandlerWithoutAnnotation))
      .build();

    service.startAndWait();
    try {
      InetSocketAddress bindAddress = service.getBindAddress();

      // Make a GET call
      URLConnection urlConn = new URL(String.format("http://%s:%d/prefix/p2/handle",
                                                    bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      urlConn.setReadTimeout(2000);

      Assert.assertEquals("Hello World", new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));

      // Make a POST call
      urlConn = new URL(String.format("http://%s:%d/prefix/p2/echo/test",
                                      bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      urlConn.setReadTimeout(2000);
      urlConn.setDoOutput(true);
      ByteStreams.copy(ByteStreams.newInputStreamSupplier("Hello".getBytes(Charsets.UTF_8)),
                       urlConn.getOutputStream());

      Assert.assertEquals("Hello test",
                          new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));

      // Ensure that even though the handler did not have a class-level annotation, we still prefix the path that it
      // handles by "/prefix"
      urlConn = new URL(String.format("http://%s:%d/prefix/ping", bindAddress.getHostName(), bindAddress.getPort()))
        .openConnection();
      urlConn.setReadTimeout(2000);

      Assert.assertEquals("OK", new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));
    } finally {
      service.stopAndWait();
    }
  }

  private abstract static class AbstractDelegatorContext<T extends HttpServiceHandler> implements DelegatorContext<T> {

    private final ThreadLocal<T> threadLocal = new ThreadLocal<T>() {
      @Override
      protected T initialValue() {
        return createHandler();
      }
    };

    @Override
    public final T getHandler() {
      return threadLocal.get();
    }

    @Override
    public final HttpServiceContext getServiceContext() {
      return new NoOpHttpServiceContext();
    }

    @Override
    public Cancellable capture() {
      threadLocal.remove();
      return new Cancellable() {
        @Override
        public void cancel() {
          // no-op
        }
      };
    }

    protected abstract T createHandler();
  }

  /**
   * An no-op implementation of {@link HttpServiceContext} that implements no-op transactional operations.
   */
  private static class NoOpHttpServiceContext implements TransactionalHttpServiceContext {

    @Override
    public HttpServiceHandlerSpecification getSpecification() {
      return null;
    }

    @Override
    public int getInstanceCount() {
      return 1;
    }

    @Override
    public int getInstanceId() {
      return 1;
    }

    @Override
    public ApplicationSpecification getApplicationSpecification() {
      return null;
    }

    @Override
    public Map<String, String> getRuntimeArguments() {
      return null;
    }

    @Override
    public String getClusterName() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
    }

    @Override
    public RunId getRunId() {
      return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return getDataset(getNamespace(), name);
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
      return getDataset(namespace, name, null);
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      return getDataset(getNamespace(), arguments);
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      throw new DatasetInstantiationException(
        String.format("Dataset '%s' cannot be instantiated. Operation not supported", name));
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      // no-op
    }

    @Override
    public void discardDataset(Dataset dataset) {
      // nop-op
    }

    @Override
    public TransactionContext newTransactionContext() {
      return new TransactionContext(null, ImmutableList.<TransactionAware>of()) {

        @Override
        public boolean addTransactionAware(TransactionAware txAware) {
          return false;
        }

        @Override
        public void start() throws TransactionFailureException {
          System.setProperty(IN_TX, "true");
        }

        @Override
        public void start(int timeout) throws TransactionFailureException {
          start();
        }

        @Override
        public void finish() throws TransactionFailureException {
          System.clearProperty(IN_TX);
        }

        @Override
        public void abort() throws TransactionFailureException {
          System.clearProperty(IN_TX);
        }

        @Override
        public void abort(TransactionFailureException cause) throws TransactionFailureException {
          System.clearProperty(IN_TX);
        }
      };
    }

    @Override
    public void dismissTransactionContext() {
      // no-op
    }

    @Override
    public int getDefaultTxTimeout() {
      return 30;
    }

    @Override
    public URL getServiceURL(String applicationId, String serviceId) {
      return null;
    }

    @Override
    public URL getServiceURL(String serviceId) {
      return null;
    }

    @Override
    public PluginProperties getPluginProperties(String pluginId) {
      return null;
    }

    @Override
    public <T> Class<T> loadPluginClass(String pluginId) {
      return null;
    }

    @Override
    public <T> T newPluginInstance(String pluginId) throws InstantiationException {
      return null;
    }

    @Nullable
    @Override
    public <T> T newPluginInstance(String pluginType, String pluginName, PluginProperties properties) {
      return null;
    }

    @Override
    public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) throws InstantiationException {
      return null;
    }

    @Override
    public Admin getAdmin() {
      return new NoopAdmin();
    }

    @Override
    public DataTracer getDataTracer(String dataTracerName) {
      return new NoopDataTracerFactory().getDataTracer(null, dataTracerName);
    }

    @Override
    public Map<String, String> listSecureData(String namespace) throws Exception {
      return null;
    }

    @Override
    public SecureStoreData getSecureData(String namespace, String name) throws Exception {
      return null;
    }

    @Override
    public void execute(TxRunnable runnable) throws TransactionFailureException {
      execute(30, runnable);
    }

    @Override
    public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
      try {
        // a poor man's way to validate whether a transaction has started
        System.setProperty(IN_TX, "true");
        runnable.run(new DatasetContext() {
          @Override
          public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T extends Dataset> T getDataset(String name,
                                                  Map<String, String> arguments) throws DatasetInstantiationException {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T extends Dataset> T getDataset(String namespace, String name,
                                                  Map<String, String> arguments) throws DatasetInstantiationException {
            throw new UnsupportedOperationException();
          }

          @Override
          public void releaseDataset(Dataset dataset) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void discardDataset(Dataset dataset) {
            throw new UnsupportedOperationException();
          }
        });
      } catch (Exception e) {
        throw new UnsupportedOperationException();
      } finally {
        System.clearProperty(IN_TX);
      }
    }

    @Override
    public MessagePublisher getMessagePublisher() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessagePublisher getDirectMessagePublisher() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageFetcher getMessageFetcher() {
      throw new UnsupportedOperationException();
    }
  }
}
