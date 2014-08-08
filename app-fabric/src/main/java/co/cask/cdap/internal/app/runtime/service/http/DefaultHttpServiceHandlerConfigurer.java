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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.service.http.HttpServiceConfigurer;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import co.cask.cdap.internal.service.http.DefaultHttpServiceSpecification;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link HttpServiceConfigurer}
 */
public class DefaultHttpServiceHandlerConfigurer implements HttpServiceConfigurer {
  private String name;
  private String description;
  private Map<String, String> arguments;

  /**
   * Instantiates the class with the given {@link HttpServiceHandler}.
   * The arguments and description are set to empty values and the name is the handler class name.
   *
   * @param handler the handler for the service
   */
  public DefaultHttpServiceHandlerConfigurer(HttpServiceHandler handler) {
    this.name = handler.getClass().getSimpleName();
    this.description = "";
    this.arguments = Collections.emptyMap();
  }

  /**
   * Sets the name.
   *
   * @param name the HTTP Service name
   */
  @Override
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets the descriptions.
   *
   * @param description the HTTP Service description
   */
  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Sets the runtime arguments.
   *
   * @param arguments the HTTP Service runtime arguments
   */
  @Override
  public void setArguments(Map<String, String> arguments) {
    this.arguments = arguments;
  }

  /**
   * Creates a {@link HttpServiceSpecification} from the parameters stored in this class.
   *
   * @return a new specification from the parameters stored in this instance
   */
  public HttpServiceSpecification createHttpServiceSpec() {
    return new DefaultHttpServiceSpecification(name, description, arguments);
  }
}
