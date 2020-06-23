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

import com.google.gson.Gson;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.app.preview.PreviewMessage;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.proto.id.ApplicationId;

/**
 * Default implementation of {@link DataTracer}, the data are preserved using {@link PreviewStore}
 */
class DefaultDataTracer implements DataTracer {
  private static final Gson GSON = new Gson();

  private final String tracerName;
  private final ApplicationId applicationId;
  private final PreviewDataPublisher previewDataPublisher;

  DefaultDataTracer(ApplicationId applicationId, String tracerName, PreviewDataPublisher previewDataPublisher) {
    this.tracerName = tracerName;
    this.applicationId = applicationId;
    this.previewDataPublisher = previewDataPublisher;
  }

  @Override
  public void info(String propertyName, Object propertyValue) {
    PreviewDataPayload payload = new PreviewDataPayload(applicationId, tracerName, propertyName, propertyValue);
    PreviewMessage message = new PreviewMessage(PreviewMessage.Type.DATA, applicationId, GSON.toJsonTree(payload));
    previewDataPublisher.publish(applicationId, message);
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
