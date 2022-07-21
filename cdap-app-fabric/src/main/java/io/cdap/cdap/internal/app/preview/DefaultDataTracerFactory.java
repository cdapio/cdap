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
package io.cdap.cdap.internal.app.preview;

import com.google.inject.Inject;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.app.preview.DataTracerFactory;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.ApplicationId;

/**
 * Default implementation of {@link DataTracerFactory}
 */
public class DefaultDataTracerFactory implements DataTracerFactory {

  private final PreviewDataPublisher publisher;
  private final CConfiguration cConf;

  @Inject
  public DefaultDataTracerFactory(PreviewDataPublisher publisher, CConfiguration cConf) {
    this.publisher = publisher;
    this.cConf = cConf;
  }

  @Override
  public DataTracer getDataTracer(ApplicationId applicationId, String tracerName) {
    return new DefaultDataTracer(applicationId, tracerName, publisher, cConf);
  }
}
