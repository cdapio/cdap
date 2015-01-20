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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class HttpHandlerGeneratorTest {

  @Path("/v1")
  public abstract static class BaseHttpHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/handle")
    public void process(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString("Hello World");
    }
  }

  @Path("/v2")
  public static final class MyHttpHandler extends BaseHttpHandler {

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
    }

    @Path("/echo/{name}")
    @POST
    public void echo(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("name") String name) {
      responder.sendString(Charsets.UTF_8.decode(request.getContent()).toString() + " " + name);
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


  @Test
  public void testHttpHandlerGenerator() throws Exception {
    MetricsCollector noOpsMetricsCollector =
      new NoOpMetricsCollectionService().getCollector(new HashMap<String, String>());
    HttpHandlerFactory factory = new HttpHandlerFactory("/prefix", noOpsMetricsCollector);

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
      URLConnection urlConn = new URL(String.format("http://%s:%d/prefix/v2/handle",
                                                    bindAddress.getHostName(), bindAddress.getPort())).openConnection();
      urlConn.setReadTimeout(2000);

      Assert.assertEquals("Hello World", new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8));

      // Make a POST call
      urlConn = new URL(String.format("http://%s:%d/prefix/v2/echo/test",
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
    public Map<String, String> getRuntimeArguments() {
      return null;
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return getDataset(name, null);
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      throw new DatasetInstantiationException(
        String.format("Dataset '%s' cannot be instantiated. Operation not supported", name));
    }

    @Override
    public TransactionContext getTransactionContext() {
      return new TransactionContext(null, ImmutableList.<TransactionAware>of()) {

        @Override
        public void addTransactionAware(TransactionAware txAware) {
          return;
        }

        @Override
        public void start() throws TransactionFailureException {
          return;
        }

        @Override
        public void finish() throws TransactionFailureException {
          return;
        }

        @Override
        public void abort() throws TransactionFailureException {
          return;
        }

        @Override
        public void abort(TransactionFailureException cause) throws TransactionFailureException {
          return;
        }
      };
    }

    @Override
    public URL getServiceURL(String applicationId, String serviceId) {
      return null;
    }

    @Override
    public URL getServiceURL(String serviceId) {
      return null;
    }
  }
}
