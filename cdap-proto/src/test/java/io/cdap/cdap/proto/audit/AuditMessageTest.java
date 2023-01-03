/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.proto.audit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.proto.audit.payload.access.AccessPayload;
import io.cdap.cdap.proto.audit.payload.access.AccessType;
import io.cdap.cdap.proto.audit.payload.metadata.MetadataPayload;
import io.cdap.cdap.proto.codec.AuditMessageTypeAdapter;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class AuditMessageTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() { }.getType();

  @Test
  public void testCreateMessage() throws Exception {
    String dsCreateJson =
      "{\"version\":2,\"time\":1000,\"metadataEntity\":{\"details\":{\"namespace\":\"ns1\",\"dataset\":\"ds1\"}," +
        "\"type\":\"dataset\"},\"user\":\"user1\",\"type\":\"CREATE\",\"payload\":{}}";
    AuditMessage dsCreate = new AuditMessage(1000L, new NamespaceId("ns1").dataset("ds1"), "user1",
                                             AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD);
    Assert.assertEquals(jsonToMap(dsCreateJson), jsonToMap(GSON.toJson(dsCreate)));
    Assert.assertEquals(dsCreate, GSON.fromJson(dsCreateJson, AuditMessage.class));
  }

  @Test
  public void testAccessMessage() throws Exception {
    String workerAccessJson =
      "{\"version\":2,\"time\":2000,\"metadataEntity\":{\"details\":{\"namespace\":\"ns1\",\"dataset\":\"ds1\"}," +
        "\"type\":\"dataset\"},\"user\":\"user1\",\"type\":\"ACCESS\",\"payload\":{\"accessType\":\"WRITE\"," +
        "\"accessor\":{\"namespace\":\"ns1\",\"application\":\"app1\",\"version\":\"v1\",\"type\":\"Worker\"," +
        "\"program\":\"worker1\",\"run\":\"run1\",\"entity\":\"PROGRAM_RUN\"}}}";
    AuditMessage workerAccess =
      new AuditMessage(2000L, new NamespaceId("ns1").dataset("ds1"), "user1", AuditType.ACCESS,
                       new AccessPayload(AccessType.WRITE, new NamespaceId("ns1").app("app1", "v1").worker("worker1")
                         .run("run1")));
    Assert.assertEquals(jsonToMap(workerAccessJson), jsonToMap(GSON.toJson(workerAccess)));
    Assert.assertEquals(workerAccess, GSON.fromJson(workerAccessJson, AuditMessage.class));

  }

  @Test
  public void testMetadataChange() throws Exception {
    String metadataJson =
      "{\"version\":2,\"time\":3000,\"metadataEntity\": { \"details\": { \"namespace\": \"ns1\", \"application\": " +
        "\"app1\", \"version\": \"v1\" }, \"type\": \"application\" },\"user\":\"user1\",\"type\":" +
        "\"METADATA_CHANGE\",\"payload\":{" +
        "\"previous\":{\"USER\":{\"properties\":{\"uk\":\"uv\",\"uk1\":\"uv2\"},\"tags\":[\"ut1\",\"ut2\"]}," +
        "\"SYSTEM\":{\"properties\":{\"sk\":\"sv\"},\"tags\":[]}}," +
        "\"additions\":{\"SYSTEM\":{\"properties\":{\"sk\":\"sv\"},\"tags\":[\"t1\",\"t2\"]}}," +
        "\"deletions\":{\"USER\":{\"properties\":{\"uk\":\"uv\"},\"tags\":[\"ut1\"]}}}}";
    Map<String, String> userProperties = new HashMap<>();
    userProperties.put("uk", "uv");
    userProperties.put("uk1", "uv2");
    Map<String, String> systemProperties = new HashMap<>();
    systemProperties.put("sk", "sv");
    Set<String> userTags = new LinkedHashSet<>();
    userTags.add("ut1");
    userTags.add("ut2");
    Map<MetadataScope, Metadata> previous = new LinkedHashMap<>();
    previous.put(MetadataScope.USER, new Metadata(Collections.unmodifiableMap(userProperties),
                                                  Collections.unmodifiableSet(userTags)));
    previous.put(MetadataScope.SYSTEM, new Metadata(Collections.unmodifiableMap(systemProperties),
                                                    Collections.unmodifiableSet(
                                                      new LinkedHashSet<String>())));
    Map<String, String> sysPropertiesAdded = new HashMap<>();
    sysPropertiesAdded.put("sk", "sv");
    Set<String> systemTagsAdded = new LinkedHashSet<>();
    systemTagsAdded.add("t1");
    systemTagsAdded.add("t2");
    Map<MetadataScope, Metadata> additions = new HashMap<>();
    additions.put(MetadataScope.SYSTEM, new Metadata(Collections.unmodifiableMap(sysPropertiesAdded),
                                                     Collections.unmodifiableSet(systemTagsAdded)));
    Map<String, String> userPropertiesDeleted = new HashMap<>();
    userPropertiesDeleted.put("uk", "uv");
    Set<String> userTagsDeleted = new LinkedHashSet<>();
    userTagsDeleted.add("ut1");
    Map<MetadataScope, Metadata> deletions = new HashMap<>();
    deletions.put(MetadataScope.USER, new Metadata(Collections.unmodifiableMap(userPropertiesDeleted),
                                                   Collections.unmodifiableSet(userTagsDeleted)));
    AuditMessage metadataChange =
      new AuditMessage(3000L, new NamespaceId("ns1").app("app1", "v1"), "user1", AuditType.METADATA_CHANGE,
                       new MetadataPayload(previous, additions, deletions));
    Assert.assertEquals(jsonToMap(metadataJson), jsonToMap(GSON.toJson(metadataChange)));
    Assert.assertEquals(metadataChange, GSON.fromJson(metadataJson, AuditMessage.class));
  }

  private Map<String, Object> jsonToMap(String json) {
    return GSON.fromJson(json, MAP_TYPE);
  }
}
