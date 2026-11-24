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

package io.cdap.cdap.internal.app.runtime.service.http;

import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.internal.app.runtime.ThrowingRunnable;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link HttpHandlerFactory} and {@link DelayedHttpServiceResponder}.
 */
public class HttpHandlerFactoryTest {

  /**
   * Valid handler for factory generation and validation tests.
   */
  @Path("/v1")
  public static final class ValidServiceHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/ping")
    public void ping(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString("pong");
    }
  }

  /**
   * Invalid handler whose endpoint returns an unsupported non-void, non-consumer type.
   */
  public static final class InvalidReturnHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/bad")
    public String bad(HttpServiceRequest request, HttpServiceResponder responder) {
      return "invalid";
    }
  }

  private static final class NoArgHttpHandler extends AbstractHttpHandler {
  }

  private static final class ThrowingCtorHttpHandler extends AbstractHttpHandler {
    @SuppressWarnings("unused")
    public ThrowingCtorHttpHandler(DelegatorContext<?> context, MetricsContext metricsContext) {
      throw new IllegalStateException("ctor failure");
    }
  }

  @Test
  public void testCreateAndValidateHttpHandlerCachesClass() {
    HttpHandlerFactory factory = new HttpHandlerFactory("/prefix", TransactionControl.EXPLICIT);
    DelegatorContext<ValidServiceHandler> context = createDelegatorContext(new ValidServiceHandler());

    HttpHandler first = factory.createHttpHandler(
        TypeToken.of(ValidServiceHandler.class), context, new NoopMetricsContext());
    HttpHandler second = factory.createHttpHandler(
        TypeToken.of(ValidServiceHandler.class), context, new NoopMetricsContext());

    Assert.assertNotSame(first, second);
    Assert.assertSame(first.getClass(), second.getClass());

    factory.validateHttpHandler(Collections.singletonList(new ValidServiceHandler()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateHttpHandlerRejectsInvalidHandler() {
    HttpHandlerFactory factory = new HttpHandlerFactory("/prefix", TransactionControl.IMPLICIT);
    factory.validateHttpHandler(Collections.singletonList(new InvalidReturnHandler()));
  }

  @Test
  public void testCreateHttpHandlerPropagatesCheckedReflectiveException() throws Exception {
    HttpHandlerFactory factory = new HttpHandlerFactory("/prefix", TransactionControl.EXPLICIT);
    Field cacheField = HttpHandlerFactory.class.getDeclaredField("handlerClasses");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    LoadingCache<TypeToken<?>, Class<?>> cache =
        (LoadingCache<TypeToken<?>, Class<?>>) cacheField.get(factory);
    cache.put(TypeToken.of(ValidServiceHandler.class), NoArgHttpHandler.class);

    try {
      factory.createHttpHandler(
          TypeToken.of(ValidServiceHandler.class),
          createDelegatorContext(new ValidServiceHandler()),
          new NoopMetricsContext());
      Assert.fail("Expected RuntimeException wrapping NoSuchMethodException");
    } catch (RuntimeException expected) {
      Assert.assertTrue(expected.getCause() instanceof NoSuchMethodException);
    }

    cache.put(TypeToken.of(ValidServiceHandler.class), ThrowingCtorHttpHandler.class);
    try {
      factory.createHttpHandler(
          TypeToken.of(ValidServiceHandler.class),
          createDelegatorContext(new ValidServiceHandler()),
          new NoopMetricsContext());
      Assert.fail("Expected RuntimeException wrapping InvocationTargetException");
    } catch (RuntimeException expected) {
      Assert.assertNotNull(expected.getCause());
      Assert.assertTrue(expected.getCause().getCause() instanceof IllegalStateException);
    }
  }

  @Test
  public void testDelayedHttpServiceResponderExecuteAndFailure() {
    HttpResponder nettyResponder = Mockito.mock(HttpResponder.class);
    AtomicBoolean released = new AtomicBoolean(false);
    ServiceTaskExecutor taskExecutor = createTaskExecutor(released);
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);

    DelayedHttpServiceResponder responder = new DelayedHttpServiceResponder(
        nettyResponder,
        (contentProducer, executor) -> Mockito.mock(BodyProducer.class),
        taskExecutor,
        metricsContext);

    Assert.assertFalse(responder.hasBufferedResponse());
    responder.sendString("hello");
    Assert.assertTrue(responder.hasBufferedResponse());
    Assert.assertFalse(responder.hasContentProducer());

    responder.execute();
    Assert.assertTrue(released.get());

    ArgumentCaptor<ByteBuf> bodyCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    ArgumentCaptor<HttpHeaders> headersCaptor = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(nettyResponder).sendContent(
        Mockito.eq(HttpResponseStatus.OK), bodyCaptor.capture(), headersCaptor.capture());
    Assert.assertEquals("hello", bodyCaptor.getValue().toString(StandardCharsets.UTF_8));
    Assert.assertEquals(
        HttpHeaderValues.KEEP_ALIVE.toString(),
        headersCaptor.getValue().get(HttpHeaderNames.CONNECTION));
    Mockito.verify(metricsContext).increment("response.successful.count", 1);
    Mockito.verify(metricsContext).increment("requests.count", 1);

    try {
      responder.sendStatus(200);
      Assert.fail("Expected IllegalStateException after responder is closed");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testDelayedHttpServiceResponderFailureStatusMapping() {
    HttpResponder nettyResponder = Mockito.mock(HttpResponder.class);
    DelayedHttpServiceResponder responder = new DelayedHttpServiceResponder(
        nettyResponder,
        (contentProducer, executor) -> Mockito.mock(BodyProducer.class),
        createTaskExecutor(new AtomicBoolean()),
        new NoopMetricsContext());

    Exception rootCauseWrapper = new Exception("outer", new IllegalArgumentException("inner cause"));
    responder.setFailure(rootCauseWrapper);
    responder.execute(false);

    ArgumentCaptor<ByteBuf> bodyCaptor = ArgumentCaptor.forClass(ByteBuf.class);
    ArgumentCaptor<HttpHeaders> headersCaptor = ArgumentCaptor.forClass(HttpHeaders.class);
    Mockito.verify(nettyResponder).sendContent(
        Mockito.eq(HttpResponseStatus.INTERNAL_SERVER_ERROR),
        bodyCaptor.capture(),
        headersCaptor.capture());
    Assert.assertEquals(
        "Exception occurred while handling request: inner cause",
        bodyCaptor.getValue().toString(StandardCharsets.UTF_8));
    Assert.assertEquals(
        HttpHeaderValues.CLOSE.toString(),
        headersCaptor.getValue().get(HttpHeaderNames.CONNECTION));

    HttpResponder customStatusResponder = Mockito.mock(HttpResponder.class);
    DelayedHttpServiceResponder statusResponder = new DelayedHttpServiceResponder(
        customStatusResponder,
        (contentProducer, executor) -> Mockito.mock(BodyProducer.class),
        createTaskExecutor(new AtomicBoolean()),
        new NoopMetricsContext());
    statusResponder.setFailure(new CustomStatusException(409, "conflict error"));
    statusResponder.execute();
    Mockito.verify(customStatusResponder).sendContent(
        Mockito.eq(HttpResponseStatus.CONFLICT),
        Mockito.any(ByteBuf.class),
        Mockito.any(HttpHeaders.class));
  }

  private static final class CustomStatusException extends Exception implements HttpErrorStatusProvider {
    private final int statusCode;

    private CustomStatusException(int statusCode, String message) {
      super(message);
      this.statusCode = statusCode;
    }

    @Override
    public int getStatusCode() {
      return statusCode;
    }
  }

  private static <T> DelegatorContext<T> createDelegatorContext(T handler) {
    return new DelegatorContext<T>() {
      @Override
      public T getHandler() {
        return handler;
      }

      @Override
      public ServiceTaskExecutor getServiceTaskExecutor() {
        return createTaskExecutor(new AtomicBoolean());
      }

      @Override
      public Cancellable capture() {
        return () -> { };
      }
    };
  }

  private static ServiceTaskExecutor createTaskExecutor(AtomicBoolean released) {
    return new ServiceTaskExecutor() {
      @Override
      public void execute(ThrowingRunnable runnable, boolean transactional) throws Exception {
        runnable.run();
      }

      @Override
      public <T> T execute(Callable<T> callable, boolean transactional) throws Exception {
        return callable.call();
      }

      @Override
      public void releaseCallResources() {
        released.set(true);
      }

      @Override
      public Transactional getTransactional() {
        return null;
      }
    };
  }
}
