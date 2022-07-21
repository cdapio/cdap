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
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.app.preview.PreviewMessage;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.preview.PreviewJsonSerializer;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;

/**
 * Default implementation of {@link DataTracer}, the data are preserved using {@link PreviewStore}
 */
class DefaultDataTracer implements DataTracer {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(StructuredRecord.class, new PreviewJsonSerializer()).create();

  private final String tracerName;
  private final ApplicationId applicationId;
  private final PreviewDataPublisher previewDataPublisher;
  private final CConfiguration cConf;
  private final int maximumTracedRecords;

  DefaultDataTracer(ApplicationId applicationId, String tracerName, PreviewDataPublisher previewDataPublisher,
                    CConfiguration cConf) {
    this.tracerName = tracerName;
    this.applicationId = applicationId;
    this.previewDataPublisher = previewDataPublisher;
    this.cConf = cConf;
    this.maximumTracedRecords = cConf.getInt(Constants.Preview.MAX_NUM_OF_RECORDS);
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

  @Override
  public int getMaximumTracedRecords() {
    return maximumTracedRecords;
  }
}
