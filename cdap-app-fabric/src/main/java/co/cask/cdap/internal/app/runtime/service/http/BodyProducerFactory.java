/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.http.BodyProducer;

/**
 * Interface for creating {@link BodyProducer} instance from {@link HttpContentProducer}.
 * Instance of this class is used in {@link DelayedHttpServiceResponder#execute(boolean)}
 * to create {@link BodyProducer} from {@link HttpContentProducer}.
 */
interface BodyProducerFactory {

  /**
   * Creates a {@link BodyProducer} from the given {@link HttpContentProducer}.
   *
   * @param contentProducer the content producer to delegate to
   * @param serviceContext the {@link HttpServiceContext} for the service call
   */
  BodyProducer create(HttpContentProducer contentProducer, HttpServiceContext serviceContext);
}
