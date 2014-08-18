/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceConfigurer;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.security.InvalidParameterException;
import java.util.List;

/**
 * TwillApplication to run a {@link Service}.
 */
public class ServiceTwillApplication implements TwillApplication {
  private final Service service;

  /**
   * Create a TwillApplication from a {@link Service}.
   * @param service
   */
  public ServiceTwillApplication(Service service) {
    this.service = service;
  }

  @Override
  public TwillSpecification configure() {
    ServiceConfigurer configurer = new DefaultServiceConfigurer();
    service.configure(new DefaultServiceConfigurer());
    List<? extends HttpServiceHandler> serviceHandlers = configurer.getHandlers();
    if (serviceHandlers.size() == 0) {
      throw new InvalidParameterException("No handlers provided. Add handlers using configurer.");
    }
    TwillSpecification.Builder.RunnableSetter runnableSetter = TwillSpecification.Builder.with()
                                     .setName(configurer.getName())
                                     .withRunnable()
                                     .add(new HttpServiceTwillRunnable(configurer.getName(),
                                                                       serviceHandlers))
                                     .noLocalFiles();
    for (ServiceWorker worker : configurer.getWorkers()) {
      ServiceWorkerTwillRunnable runnable = new ServiceWorkerTwillRunnable(worker);
      runnableSetter = runnableSetter.add(runnable).noLocalFiles();
    }
    return runnableSetter.anyOrder().build();
  }
}
