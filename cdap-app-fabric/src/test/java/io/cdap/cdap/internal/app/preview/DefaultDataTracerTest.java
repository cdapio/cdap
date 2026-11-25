/*
 * Copyright © 2023 Cask Data, Inc.
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
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.preview.PreviewDataPublisher;
import io.cdap.cdap.app.preview.PreviewMessage;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DefaultDataTracer}.
 */
public class DefaultDataTracerTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testTracerPublishesStructuredRecordAndSkipsInternalJdkClasses() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Preview.MAX_NUM_OF_RECORDS, 42);
    ApplicationId appId = NamespaceId.DEFAULT.app("previewApp");

    List<PreviewMessage> publishedMessages = new ArrayList<>();
    PreviewDataPublisher publisher = new PreviewDataPublisher() {
      @Override
      public void publish(EntityId entityId, PreviewMessage previewMessage) {
        Assert.assertEquals(appId, entityId);
        publishedMessages.add(previewMessage);
      }

      @Override
      public void setPublisherInfo(byte[] publisherInfo) {
        // no-op
      }
    };

    DefaultDataTracer tracer = new DefaultDataTracer(appId, "stageOne", publisher, cConf);
    Assert.assertEquals("stageOne", tracer.getName());
    Assert.assertTrue(tracer.isEnabled());
    Assert.assertEquals(42, tracer.getMaximumTracedRecords());

    Schema schema = Schema.recordOf("rec", Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    StructuredRecord record = StructuredRecord.builder(schema).set("id", 7).build();
    tracer.info("records.out", record);

    Assert.assertEquals(1, publishedMessages.size());
    PreviewMessage first = publishedMessages.get(0);
    Assert.assertEquals(PreviewMessage.Type.DATA, first.getType());
    Assert.assertEquals(appId, first.getEntityId());
    JsonObject firstJson = first.<JsonObject>getPayload(GSON, JsonObject.class);
    Assert.assertEquals("stageOne", firstJson.get("tracerName").getAsString());
    Assert.assertEquals("records.out", firstJson.get("propertyName").getAsString());
    Assert.assertEquals(7,
        firstJson.getAsJsonObject("propertyValue").getAsJsonObject("fields").get("id").getAsInt());

    // System ClassLoader is under sun.* (JDK 8) or jdk.internal.* (JDK 9+), exercising ExclusionStrategy.
    HolderWithInternalField holder =
        new HolderWithInternalField("visible", ClassLoader.getSystemClassLoader());
    tracer.info("custom", holder);

    Assert.assertEquals(2, publishedMessages.size());
    JsonObject secondJson = publishedMessages.get(1).<JsonObject>getPayload(GSON, JsonObject.class);
    JsonObject secondValue = secondJson.getAsJsonObject("propertyValue");
    Assert.assertEquals("visible", secondValue.get("label").getAsString());
    Assert.assertFalse(secondValue.has("internalObject"));
  }

  private static final class HolderWithInternalField {

    private final String label;
    @SuppressWarnings("unused")
    private final Object internalObject;

    private HolderWithInternalField(String label, Object internalObject) {
      this.label = label;
      this.internalObject = internalObject;
    }
  }
}
