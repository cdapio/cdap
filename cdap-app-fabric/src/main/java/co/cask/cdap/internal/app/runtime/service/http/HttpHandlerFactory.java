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

import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.asm.ByteCodeClassLoader;
import co.cask.cdap.internal.asm.ClassDefinition;
import co.cask.http.HttpHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * A factory for creating {@link co.cask.http.HttpHandler} from user provided instance of
 * {@link HttpServiceHandler}.
 */
public final class HttpHandlerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HttpHandlerFactory.class);

  private final LoadingCache<TypeToken<? extends HttpServiceHandler>, Class<?>> handlerClasses;
  private final MetricsCollectionService metricsCollectionService;
  private final String metricsScope;
  private final String runId;

  /**
   * Creates an instance that could generate {@link HttpHandler} that always binds to service Path that starts with
   * the given prefix.
   */
  public HttpHandlerFactory(final String pathPrefix, String runId,
                            final MetricsCollectionService metricsCollectionService, final String metricsScope) {
    this.runId = runId;
    this.metricsCollectionService = metricsCollectionService;
    this.metricsScope = metricsScope;
    handlerClasses = CacheBuilder.newBuilder().build(
      new CacheLoader<TypeToken<? extends HttpServiceHandler>, Class<?>>() {
      @Override
      public Class<?> load(TypeToken<? extends HttpServiceHandler> key) throws Exception {
        // Generate the new class if it hasn't before and load it through a ByteCodeClassLoader.
        ClassDefinition classDefinition = new HttpHandlerGenerator().generate(key, pathPrefix);

        // The ClassLoader of the generated HttpHandler has CDAP system ClassLoader as parent.
        // The ClassDefinition contains list of classes that should not be loaded by the generated class ClassLoader
        ByteCodeClassLoader classLoader = new ByteCodeClassLoader(HttpHandlerFactory.class.getClassLoader());
        classLoader.addClass(classDefinition);
        return classLoader.loadClass(classDefinition.getClassName());
      }
    });
  }

  /**
   * Creates an implementation of {@link HttpHandler} that delegates all public {@link javax.ws.rs.Path @Path} methods
   * to the user delegate.
   */
  public <T extends HttpServiceHandler> HttpHandler createHttpHandler(TypeToken<T> delegateType,
                                                                      DelegatorContext<T> context) {
    Class<?> cls = handlerClasses.getUnchecked(delegateType);
    Preconditions.checkState(HttpHandler.class.isAssignableFrom(cls),
                             "Fatal error: %s is not instance of %s", cls, HttpHandler.class);

    @SuppressWarnings("unchecked")
    Class<? extends HttpHandler> handlerClass = (Class<? extends HttpHandler>) cls;

    try {
      Constructor<? extends HttpHandler> constuctor = handlerClass.getConstructor(DelegatorContext.class,
                                                                                  MetricsCollector.class);
      MetricsCollector metricsCollector = metricsCollectionService.getCollector(MetricsScope.SYSTEM,
                                                                                metricsScope, runId);
      return constuctor.newInstance(context, metricsCollector);
    } catch (Exception e) {
      LOG.error("Failed to instantiate generated HttpHandler {}", handlerClass, e);
      throw Throwables.propagate(e);
    }
  }
}
