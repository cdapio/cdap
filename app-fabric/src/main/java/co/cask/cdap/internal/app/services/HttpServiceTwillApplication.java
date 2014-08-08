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

import co.cask.cdap.api.service.http.HttpServiceHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

/**
 * A Twill Application containing a single runnable that runs an
 * HTTP Service using handlers passed to the constructor.
 */
public class HttpServiceTwillApplication implements TwillApplication {
  private final String name;
  private final Iterable<HttpServiceHandler> handlers;

  /**
   * Instantiates the class with the given name and {@link HttpServiceHandler}s. The name is the
   * name used when announcing this service. The handlers will handle the HTTP requests.
   *
   * @param name the name of the service used when announcing the service
   * @param handlers the handlers of the HTTP request
   */
  public HttpServiceTwillApplication(String name, Iterable<HttpServiceHandler> handlers) {
    this.name = name;
    this.handlers = handlers;
  }

  /**
   * Configures this Twill application with one runnable, a {@link HttpServiceTwillRunnable}
   *
   * @return the specification which describes this application
   */
  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
      .setName(name)
      .withRunnable()
      .add(new HttpServiceTwillRunnable(name, handlers))
      .noLocalFiles()
      .anyOrder()
      .build();
  }
}
