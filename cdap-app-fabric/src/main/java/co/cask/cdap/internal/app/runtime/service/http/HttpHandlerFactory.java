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

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.internal.asm.ByteCodeClassLoader;
import co.cask.cdap.internal.asm.ClassDefinition;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * A factory for creating {@link co.cask.http.HttpHandler} from user http service handler
 */
public final class HttpHandlerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HttpHandlerFactory.class);

  private final LoadingCache<TypeToken<?>, Class<?>> handlerClasses;

  /**
   * Creates an instance that could generate {@link HttpHandler} that always binds to service Path that starts with
   * the given prefix.
   */
  public HttpHandlerFactory(String pathPrefix, TransactionControl defaultTxControl) {
    handlerClasses = CacheBuilder.newBuilder().build(
      new CacheLoader<TypeToken<?>, Class<?>>() {
      @Override
      public Class<?> load(TypeToken<?> key) throws Exception {
        // Generate the new class if it hasn't before and load it through a ByteCodeClassLoader.
        ClassDefinition classDef = new HttpHandlerGenerator(defaultTxControl).generate(key, pathPrefix);

        // The ClassLoader of the generated HttpHandler has CDAP system ClassLoader as parent.
        // The ClassDefinition contains list of classes that should not be loaded by the generated class ClassLoader
        ByteCodeClassLoader classLoader = new ByteCodeClassLoader(HttpHandlerFactory.class.getClassLoader());
        classLoader.addClass(classDef);
        return classLoader.loadClass(classDef.getClassName());
      }
    });
  }

  /**
   * Creates an implementation of {@link HttpHandler} that delegates all public {@link javax.ws.rs.Path @Path} methods
   * to the user delegate.
   */
  public <T> HttpHandler createHttpHandler(TypeToken<T> delegateType, DelegatorContext<T> context,
                                           MetricsContext metricsContext) {
    Class<?> cls = handlerClasses.getUnchecked(delegateType);
    Preconditions.checkState(HttpHandler.class.isAssignableFrom(cls),
                             "Fatal error: %s is not instance of %s", cls, HttpHandler.class);

    @SuppressWarnings("unchecked")
    Class<? extends HttpHandler> handlerClass = (Class<? extends HttpHandler>) cls;

    try {
      Constructor<? extends HttpHandler> constructor = handlerClass.getConstructor(DelegatorContext.class,
                                                                                   MetricsContext.class);
      return constructor.newInstance(context, metricsContext);
    } catch (Exception e) {
      LOG.error("Failed to instantiate generated HttpHandler {}", handlerClass, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Validates the given set of user service handlers.
   *
   * @param handlers set of service handlers to validate.
   * @param <T> type of the handler
   * @throws IllegalArgumentException if any of the service handler is not valid
   */
  public <T> void validateHttpHandler(Iterable<T> handlers) {
    List<HttpHandler> httpHandlers = new ArrayList<>();
    NoopMetricsContext metricsContext = new NoopMetricsContext();

    for (T handler : handlers) {
      try {
        @SuppressWarnings("unchecked")
        TypeToken<T> type = (TypeToken<T>) TypeToken.of(handler.getClass());
        httpHandlers.add(createHttpHandler(type, new VerificationDelegateContext<>(handler), metricsContext));
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid http handler class " + handler.getClass().getName());
      }
    }

    try {
      // Constructs a NettyHttpService, to verify that the handlers passed in by the user are valid.
      NettyHttpService.builder("service-configurer")
        .setHttpHandlers(httpHandlers)
        .build();
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid http handler", e);
    }
  }

  /**
   * A dummy {@link DelegatorContext} for the {@link #validateHttpHandler(Iterable)} method to use.
   *
   * @param <T> type of the user http handler
   */
  private static final class VerificationDelegateContext<T> implements DelegatorContext<T> {

    private final T handler;

    private VerificationDelegateContext(T handler) {
      this.handler = handler;
    }

    @Override
    public T getHandler() {
      return handler;
    }

    @Override
    public ServiceTaskExecutor getServiceTaskExecutor() {
      // Never used. (It's only used during server runtime, which we don't verify).
      return null;
    }

    @Override
    public Cancellable capture() {
      return new Cancellable() {
        @Override
        public void cancel() {
          // no-op
        }
      };
    }
  }
}
