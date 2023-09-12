/*
 * Copyright © 2015-2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.element;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.QueryId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.id.SupportBundleEntityId;
import io.cdap.cdap.proto.id.SystemAppEntityId;
import io.cdap.cdap.proto.id.SystemServiceId;
import io.cdap.cdap.proto.id.TopicId;
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
  APPLICATIONREFERENCE(ApplicationReference.class),
  APPLICATION(ApplicationId.class),
  PROGRAMREFERENCE(ProgramReference.class),
  PROGRAM(ProgramId.class),
  PROGRAM_RUN(ProgramRunId.class),

  DATASET_TYPE(DatasetTypeId.class),
  DATASET_MODULE(DatasetModuleId.class),
  SCHEDULE(ScheduleId.class),
  ARTIFACT(ArtifactId.class),
  PLUGIN(PluginId.class),
  DATASET(DatasetId.class),
  SECUREKEY(SecureKeyId.class),
  TOPIC(TopicId.class),

  PROFILE(ProfileId.class),

  QUERY(QueryId.class),
  SUPPORT_BUNDLE(SupportBundleEntityId.class),
  SYSTEM_SERVICE(SystemServiceId.class),
  SYSTEM_APP_ENTITY(SystemAppEntityId.class),

  CREDENTIAL_PROFILE(CredentialProfileId.class),
  CREDENTIAL_IDENTITY(CredentialIdentityId.class),
  OPERATION_RUN(OperationRunId.class);

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

  /**
   * Constructs the entity ID from an ID parts iterable.
   *
   * @param idParts The components of the ID.
   * @param <T> The entity type.
   * @return An instance of the entity ID.
   */
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
