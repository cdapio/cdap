/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.app.preview.DataTracerFactory;
import co.cask.cdap.proto.id.ApplicationId;

/**
 * Implementation of {@link DataTracerFactory} which creates {@link NoopDataTracer}
 */
public class NoopDataTracerFactory implements DataTracerFactory {

  @Override
  public DataTracer getDataTracer(ApplicationId applicationId, String tracerName) {
    return new NoopDataTracer(tracerName);
  }
}
