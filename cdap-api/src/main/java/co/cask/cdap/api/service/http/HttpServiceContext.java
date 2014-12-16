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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.DatasetContext;

/**
 * The context for a {@link HttpServiceHandler}. Currently contains methods to receive the
 * {@link HttpServiceHandlerSpecification} and the runtime arguments passed by the user.
 */
public interface HttpServiceContext extends RuntimeContext, DatasetContext, ServiceDiscoverer {

  /**
   * @return the specification bound to this HttpServiceContext
   */
  HttpServiceHandlerSpecification getSpecification();
}
