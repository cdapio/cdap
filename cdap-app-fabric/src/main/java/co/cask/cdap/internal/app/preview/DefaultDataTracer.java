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
import co.cask.cdap.app.store.preview.PreviewStore;
import co.cask.cdap.proto.id.ApplicationId;

/**
 * Default implementation of {@link DataTracer}, the data are preserved using {@link PreviewStore}
 */
class DefaultDataTracer implements DataTracer {

  private final String tracerName;
  private final ApplicationId applicationId;
  private final PreviewStore previewStore;

  DefaultDataTracer(ApplicationId applicationId, String tracerName, PreviewStore previewStore) {
    this.tracerName = tracerName;
    this.applicationId = applicationId;
    this.previewStore = previewStore;
  }

  @Override
  public void info(String propertyName, Object propertyValue) {
    previewStore.put(applicationId, tracerName, propertyName, propertyValue);
  }

  @Override
  public String getName() {
    return tracerName;
  }

  @Override
  public boolean isEnabled() {
    return true;
  }
}
