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

package com.continuuity.internal.app.services;

import com.continuuity.api.service.http.HttpServiceHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Twill Application which runs a Netty Http Service with the handlers passed into the constructor.
 */
public class HttpServiceTwillApp implements TwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);

  String name;
  Iterable<HttpServiceHandler> handlers;

  public HttpServiceTwillApp(String name, Iterable<HttpServiceHandler> handlers) {
    this.name = name;
    this.handlers = handlers;
  }

  @Override
  public TwillSpecification configure() {
    LOG.debug("GOT HTTP SERVICE APP CONFIGURE");
    return TwillSpecification.Builder.with()
      .setName(name)
      .withRunnable()
      .add(new HttpServiceTwillRunnable(name, handlers))
      .noLocalFiles()
      .anyOrder()
      .build();
  }
}
