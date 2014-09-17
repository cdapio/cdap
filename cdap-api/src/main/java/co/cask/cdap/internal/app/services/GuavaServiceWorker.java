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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.ServiceWorkerContext;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * Wrapper around a Guava-Service, to allow adding guava services as workers to a user-defined service.
 */
public final class GuavaServiceWorker extends AbstractServiceWorker {
  private static final Logger LOG = LoggerFactory.getLogger(GuavaServiceWorker.class);

  private Service delegate;
  private Future<Service.State> completion;

  public GuavaServiceWorker(Service service) {
    this.delegate = service;
  }

  Service getDelegate() {
    return delegate;
  }

  void setDelegate(Service service) {
    this.delegate = service;
  }

  @Override
  public void initialize(ServiceWorkerContext context) throws Exception {
    completion = Services.getCompletionFuture(delegate);
    delegate.start();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public void run() {
    try {
      completion.get();
    } catch (Exception e) {
      LOG.error("Caught exception while running guava service: ", e);
    }
  }
}
