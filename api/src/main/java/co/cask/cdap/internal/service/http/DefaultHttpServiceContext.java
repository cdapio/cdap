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

package co.cask.cdap.internal.service.http;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceSpecification;

import java.util.Map;

/**
 * Default implementation of HttpServiceContext which simply stores and retrieves the
 * spec provided when this class is instantiated
 */
public class DefaultHttpServiceContext implements HttpServiceContext {

  private final HttpServiceSpecification spec;
  private final Map<String, String> runtimeArgs;

  /**
   * Instantiates the context with a spec and a map for the runtime arguments
   *
   * @param spec the {@link HttpServiceSpecification} for this context
   * @param runtimeArgs the runtime arguments as a map of string to string
   */
  public DefaultHttpServiceContext(HttpServiceSpecification spec, Map<String, String> runtimeArgs) {
    this.spec = spec;
    this.runtimeArgs = runtimeArgs;
  }

  /**
   * @param spec the {@link HttpServiceSpecification} for this context
   */
  public DefaultHttpServiceContext(HttpServiceSpecification spec, String[] runtimeArgs) {
    this.spec = spec;
    this.runtimeArgs = RuntimeArguments.fromPosixArray(runtimeArgs);
  }

  /**
   * @return the {@link HttpServiceSpecification} for this context
   */
  @Override
  public HttpServiceSpecification getSpecification() {
    return spec;
  }

  /**
   * @return the runtime arguments for the {@link HttpServiceContext}
   */
  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }
}
