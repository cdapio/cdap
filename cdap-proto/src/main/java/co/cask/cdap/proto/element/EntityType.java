/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.element;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.FlowletQueueId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.QueryId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.id.SystemServiceId;
import co.cask.cdap.proto.id.TopicId;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents a type of CDAP element. E.g. namespace, application, datasets, streams.
 */
// TODO: remove duplication with EntityType in cdap-cli
@SuppressWarnings("unchecked")
public enum EntityType {

  INSTANCE(InstanceId.class, null),
  KERBEROSPRINCIPAL(KerberosPrincipalId.class, null),
  NAMESPACE(NamespaceId.class, Id.Namespace.class),
  APPLICATION(ApplicationId.class, Id.Application.class),
  PROGRAM(ProgramId.class, Id.Program.class),
  PROGRAM_RUN(ProgramRunId.class, Id.Program.Run.class),

  STREAM(StreamId.class, Id.Stream.class),
  STREAM_VIEW(StreamViewId.class, Id.Stream.View.class),

  DATASET_TYPE(DatasetTypeId.class, Id.DatasetType.class),
  DATASET_MODULE(DatasetModuleId.class, Id.DatasetModule.class),
  FLOWLET(FlowletId.class, Id.Flow.Flowlet.class),
  FLOWLET_QUEUE(FlowletQueueId.class, Id.Flow.Flowlet.Queue.class),
  SCHEDULE(ScheduleId.class, Id.Schedule.class),
  NOTIFICATION_FEED(NotificationFeedId.class, Id.NotificationFeed.class),
  ARTIFACT(ArtifactId.class, Id.Artifact.class),
  DATASET(DatasetId.class, Id.DatasetInstance.class),
  SECUREKEY(SecureKeyId.class, null),
  TOPIC(TopicId.class, null),

  QUERY(QueryId.class, Id.QueryHandle.class),
  SYSTEM_SERVICE(SystemServiceId.class, Id.SystemService.class);

  private static final Map<Class<? extends EntityId>, EntityType> byIdClass;
  private static final Map<Class<? extends Id>, EntityType> byOldIdClass;
  static {
    Map<Class<? extends EntityId>, EntityType> byIdClassMap = new LinkedHashMap<>();
    Map<Class<? extends Id>, EntityType> byOldIdClassMap = new LinkedHashMap<>();
    for (EntityType type : EntityType.values()) {
      byIdClassMap.put(type.getIdClass(), type);
      if (type.getOldIdClass() != null) {
        byOldIdClassMap.put(type.getOldIdClass(), type);
      }
    }
    byIdClass = Collections.unmodifiableMap(byIdClassMap);
    byOldIdClass = Collections.unmodifiableMap(byOldIdClassMap);
  }

  private final Class<? extends EntityId> idClass;
  @Nullable
  private final Class<? extends Id> oldIdClass;
  private final MethodHandle fromIdParts;

  EntityType(Class<? extends EntityId> idClass, Class<? extends Id> oldIdClass) {
    this.idClass = idClass;
    this.oldIdClass = oldIdClass;
    try {
      this.fromIdParts = MethodHandles.lookup()
        .findStatic(idClass, "fromIdParts", MethodType.methodType(idClass, Iterable.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException("Failed to initialize EntityType", e);
    }
  }

  public Class<? extends EntityId> getIdClass() {
    return idClass;
  }

  @Nullable
  public Class<? extends Id> getOldIdClass() {
    return oldIdClass;
  }

  public static EntityType valueOfIdClass(Class<? extends EntityId> idClass) {
    if (!byIdClass.containsKey(idClass)) {
      throw new IllegalArgumentException("No EntityType registered for ID class: " + idClass.getName());
    }
    return byIdClass.get(idClass);
  }

  public static EntityType valueOfOldIdClass(Class<? extends Id> oldIdClass) {
    if (!byOldIdClass.containsKey(oldIdClass)) {
      throw new IllegalArgumentException("No EntityType registered for old ID class: " + oldIdClass.getName());
    }
    return byOldIdClass.get(oldIdClass);
  }

  public <T extends EntityId> T fromIdParts(Iterable<String> idParts) {
    try {
      return (T) fromIdParts.invoke(idParts);
    } catch (RuntimeException t) {
      throw t;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
