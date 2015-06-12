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

package co.cask.cdap.api.service;

import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Service} that composes of one or more {@link HttpServiceHandler}.
 */
public class BasicService extends AbstractService {

  private final String name;
  private final List<HttpServiceHandler> handlers;

  public BasicService(String name, HttpServiceHandler handler, HttpServiceHandler...handlers) {
    this.name = name;
    this.handlers = new ArrayList<>();
    this.handlers.add(handler);
    this.handlers.addAll(Arrays.asList(handlers));
  }

  public BasicService(String name, Iterable<? extends HttpServiceHandler> handlers) {
    this.name = name;
    this.handlers = new ArrayList<>();
    for (HttpServiceHandler handler : handlers) {
      this.handlers.add(handler);
    }
  }

  @Override
  protected void configure() {
    setName(name);
    addHandlers(handlers);
  }
}
