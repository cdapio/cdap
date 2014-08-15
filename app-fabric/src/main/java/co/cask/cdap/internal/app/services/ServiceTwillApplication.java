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

import co.cask.cdap.api.service.DefaultServiceWorkerContext;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import com.google.common.base.Preconditions;
import com.sun.tools.javac.util.List;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillSpecification;

/**
 * TwillApplication to run custom user Services.
 */
public class ServiceTwillApplication implements TwillApplication {
  private final Service service;

  /**
   * Create a TwillApplication from a custom user Service.
   * @param service
   */
  public ServiceTwillApplication(Service service) {
    this.service = service;
  }

  @Override
  public TwillSpecification configure() {
    service.configure(new DefaultServiceConfigurer());
    HttpServiceHandler serviceHandler = service.getHandler();
    Preconditions.checkNotNull(serviceHandler, "No ServiceHandler found. Add a handler using the configurer.");
    TwillSpecification.Builder.RunnableSetter runnableSetter = TwillSpecification.Builder.with()
                                     .setName(service.getName())
                                     .withRunnable()
                                     .add(new HttpServiceTwillRunnable(service.getName(), List.of(serviceHandler)))
                                     .noLocalFiles();
    for (ServiceWorker worker : service.getWorkers()) {
      worker.configure(new DefaultServiceWorkerContext(worker.getRuntimeArguments()));
      runnableSetter.add((TwillRunnable) worker);
    }
    return runnableSetter.anyOrder().build();
  }
}
