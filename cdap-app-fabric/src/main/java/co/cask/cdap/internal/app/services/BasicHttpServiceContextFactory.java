/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.internal.app.runtime.service.http.BasicHttpServiceContext;

import javax.annotation.Nullable;

/**
 * Factory for creating {@link BasicHttpServiceContext}.
 */
public interface BasicHttpServiceContextFactory {

  /**
   * Creates a new instance of {@link BasicHttpServiceContext} with the given spec.
   *
   * @param spec the specification of the http handler, or {@code null} if the context created is not
   *             associated with any handler.
   */
  BasicHttpServiceContext create(@Nullable HttpServiceHandlerSpecification spec);
}
