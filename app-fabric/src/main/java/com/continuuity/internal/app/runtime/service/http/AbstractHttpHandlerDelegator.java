/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.service.http;

import com.continuuity.api.service.http.HttpServiceContext;
import com.continuuity.api.service.http.HttpServiceHandler;
import com.continuuity.api.service.http.HttpServiceRequest;
import com.continuuity.api.service.http.HttpServiceResponder;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Throwables;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 *
 */
public abstract class AbstractHttpHandlerDelegator implements HttpHandler {

  private final HttpServiceHandler delegate;
  private final HttpServiceContext context;

  protected AbstractHttpHandlerDelegator(HttpServiceHandler delegate, HttpServiceContext context) {
    this.delegate = delegate;
    this.context = context;
  }

  @Override
  public void init(HandlerContext context) {
    try {
      delegate.initialize(this.context);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void destroy(HandlerContext context) {
    delegate.destroy();
  }

  protected final HttpServiceRequest wrapRequest(HttpRequest request) {
    return new DefaultHttpServiceRequest(request);
  }

  protected final HttpServiceResponder wrapResponder(HttpResponder responder) {
    return new DefaultHttpServiceResponder(responder);
  }
}
