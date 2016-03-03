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

package co.cask.cdap.proto.audit;

import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.audit.payload.metadata.MetadataRecord;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class AuditMessageTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  @Test
  public void testCreateMessage() throws Exception {
    String dsCreateJson =
      "{\"time\":1000,\"entityId\":{\"namespace\":\"ns1\",\"dataset\":\"ds1\",\"entity\":\"DATASET\"}," +
        "\"user\":\"user1\",\"type\":\"CREATE\",\"payload\":{}}";
    AuditMessage dsCreate = new AuditMessage(1000L, Ids.namespace("ns1").dataset("ds1"), "user1",
                                             MessageType.CREATE, AuditPayload.EMPTY_PAYLOAD);
    Assert.assertEquals(dsCreateJson, GSON.toJson(dsCreate));
    Assert.assertEquals(dsCreate, GSON.fromJson(dsCreateJson, AuditMessage.class));
  }

  @Test
  public void testAccessMessage() throws Exception {
    String accessJson =
      "{\"time\":2000,\"entityId\":{\"namespace\":\"ns1\",\"stream\":\"stream1\",\"entity\":\"STREAM\"}," +
        "\"user\":\"user1\",\"type\":\"ACCESS\",\"payload\":{\"accessType\":\"WRITE\"," +
        "\"programRun\":{\"namespace\":\"ns1\",\"application\":\"app1\",\"type\":\"Flow\",\"program\":\"flow1\"," +
        "\"run\":\"run1\",\"entity\":\"PROGRAM_RUN\"}}}";
    AuditMessage access =
      new AuditMessage(2000L, Ids.namespace("ns1").stream("stream1"), "user1", MessageType.ACCESS,
                       new AccessPayload(AccessType.WRITE, Ids.namespace("ns1").app("app1").flow("flow1").run("run1")));
    Assert.assertEquals(accessJson, GSON.toJson(access));
    Assert.assertEquals(access, GSON.fromJson(accessJson, AuditMessage.class));
  }

  @Test
  public void testMetadataChange() throws Exception {
    String metadataJson =
      "{\"time\":3000,\"entityId\":{\"namespace\":\"ns1\",\"application\":\"app1\",\"entity\":\"APPLICATION\"}," +
        "\"user\":\"user1\",\"type\":\"METADATA_CHANGE\",\"payload\":{" +
        "\"previous\":{\"USER\":{\"properties\":{\"uk\":\"uv\",\"uk1\":\"uv2\"},\"tags\":[\"ut1\",\"ut2\"]}," +
        "\"SYSTEM\":{\"properties\":{\"sk\":\"sv\"},\"tags\":[]}}," +
        "\"additions\":{\"SYSTEM\":{\"properties\":{\"sk\":\"sv\"},\"tags\":[\"t1\",\"t2\"]}}," +
        "\"deletions\":{\"USER\":{\"properties\":{\"uk\":\"uv\"},\"tags\":[\"ut1\"]}}}}";
    Map<MetadataScope, MetadataRecord> previous = ImmutableMap.of(MetadataScope.USER,
                                                                  new MetadataRecord(ImmutableMap.of("uk", "uv",
                                                                                                     "uk1", "uv2"),
                                                                                     ImmutableSet.of("ut1", "ut2")),
                                                                  MetadataScope.SYSTEM,
                                                                  new MetadataRecord(ImmutableMap.of("sk", "sv"),
                                                                                     ImmutableSet.<String>of()));
    Map<MetadataScope, MetadataRecord> additions = ImmutableMap.of(MetadataScope.SYSTEM,
                                                                  new MetadataRecord(ImmutableMap.of("sk", "sv"),
                                                                                     ImmutableSet.of("t1", "t2")));
    Map<MetadataScope, MetadataRecord> deletions = ImmutableMap.of(MetadataScope.USER,
                                                                  new MetadataRecord(ImmutableMap.of("uk", "uv"),
                                                                                     ImmutableSet.of("ut1")));
    AuditMessage metadataChange =
      new AuditMessage(3000L, Ids.namespace("ns1").app("app1"), "user1", MessageType.METADATA_CHANGE,
                       new MetadataPayload(previous, additions, deletions));
    Assert.assertEquals(metadataJson, GSON.toJson(metadataChange));
    Assert.assertEquals(metadataChange, GSON.fromJson(metadataJson, AuditMessage.class));
  }
}
