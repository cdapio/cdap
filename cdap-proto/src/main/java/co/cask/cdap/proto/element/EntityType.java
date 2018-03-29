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
import co.cask.cdap.proto.id.ProfileId;
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
import javax.annotation.Nullable;

/**
 * Represents a type of CDAP element. E.g. namespace, application, datasets, streams.
 */
// TODO: remove duplication with EntityType in cdap-cli
@SuppressWarnings("unchecked")
public enum EntityType {

  INSTANCE(InstanceId.class),
  KERBEROSPRINCIPAL(KerberosPrincipalId.class),
  NAMESPACE(NamespaceId.class),
  APPLICATION(ApplicationId.class),
  PROGRAM(ProgramId.class),
  PROGRAM_RUN(ProgramRunId.class),

  STREAM(StreamId.class),
  STREAM_VIEW(StreamViewId.class),

  DATASET_TYPE(DatasetTypeId.class),
  DATASET_MODULE(DatasetModuleId.class),
  FLOWLET(FlowletId.class),
  FLOWLET_QUEUE(FlowletQueueId.class),
  SCHEDULE(ScheduleId.class),
  NOTIFICATION_FEED(NotificationFeedId.class),
  ARTIFACT(ArtifactId.class),
  DATASET(DatasetId.class),
  SECUREKEY(SecureKeyId.class),
  TOPIC(TopicId.class),

  PROFILE(ProfileId.class),

  QUERY(QueryId.class),
  SYSTEM_SERVICE(SystemServiceId.class);

  private final Class<? extends EntityId> idClass;
  @Nullable
  private final MethodHandle fromIdParts;

  EntityType(Class<? extends EntityId> idClass) {
    this.idClass = idClass;
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
