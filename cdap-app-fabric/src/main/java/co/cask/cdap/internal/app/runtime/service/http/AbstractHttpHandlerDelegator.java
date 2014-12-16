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

import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionContext;
import com.google.common.base.Preconditions;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * An abstract base class for all {@link HttpHandler} generated through the {@link HttpHandlerGenerator}.
 *
 * @param <T> Type of the user {@link HttpServiceHandler}.
 */
public abstract class AbstractHttpHandlerDelegator<T extends HttpServiceHandler> implements HttpHandler,
                                                                                            DelegatorContext<T> {

  private final DelegatorContext<T> context;
  private MetricsCollector metricsCollector;

  protected AbstractHttpHandlerDelegator(DelegatorContext<T> context, MetricsCollector metricsCollector) {
    this.context = context;
    this.metricsCollector = metricsCollector;
  }

  @Override
  public void init(HandlerContext context) {
  }

  @Override
  public void destroy(HandlerContext context) {
  }

  @Override
  public final T getHandler() {
    return context.getHandler();
  }

  @Override
  public HttpServiceContext getServiceContext() {
    return context.getServiceContext();
  }

  protected final TransactionContext getTransactionContext() {
    Preconditions.checkState(context.getServiceContext() instanceof TransactionalHttpServiceContext,
                             "This instance of HttpServiceContext does not support transactions.");
    return ((TransactionalHttpServiceContext) context.getServiceContext()).getTransactionContext();
  }

  protected final HttpServiceRequest wrapRequest(HttpRequest request) {
    return new DefaultHttpServiceRequest(request);
  }

  protected final HttpServiceResponder wrapResponder(HttpResponder responder) {
    return new DefaultHttpServiceResponder(responder, metricsCollector);
  }
}
