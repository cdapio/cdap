/*
 * Copyright © 2018 Cask Data, Inc.
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


package co.cask.cdap.metadata.profile;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.config.ConfigDataset;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.writer.MetadataMessage;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.ApplicationMeta;
import co.cask.cdap.metadata.MetadataMessageProcessor;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.store.NamespaceMDS;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Set;

/**
 * Class to process the profile metadata request
 */
public class ProfileMetadataMessageProcessor implements MetadataMessageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileMetadataMessageProcessor.class);
  private static final Type SET_ENTITY_TYPE = new TypeToken<Set<EntityId>>() { }.getType();

  private static final Gson GSON = new GsonBuilder()
                                     .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
                                     .create();

  private final NamespaceMDS namespaceMDS;
  private final AppMetadataStore appMetadataStore;
  private final ProgramScheduleStoreDataset scheduleDataset;
  private final ConfigDataset configDataset;
  private final MetadataDataset metadataDataset;

  public ProfileMetadataMessageProcessor(CConfiguration cConf, DatasetContext datasetContext,
                                         DatasetFramework datasetFramework) {
    namespaceMDS = NamespaceMDS.getNamespaceMDS(datasetContext, datasetFramework);
    appMetadataStore = AppMetadataStore.create(cConf, datasetContext, datasetFramework);
    scheduleDataset = Schedulers.getScheduleStore(datasetContext, datasetFramework);
    configDataset = ConfigDataset.get(datasetContext, datasetFramework);
    metadataDataset = DefaultMetadataStore.getMetadataDataset(datasetFramework, MetadataScope.SYSTEM);
  }

  @Override
  public void processMessage(MetadataMessage message) {
    EntityId entityId = message.getEntityId();

    switch (message.getType()) {
      case PROFILE_UPDATE:
        updateProfileMetadata(entityId, message);
        break;
      case PROILE_REMOVE:
        removeProfileMetadata(entityId, message.getPayload(GSON, SET_ENTITY_TYPE));
        break;
      default:
        // This shouldn't happen
        LOG.warn("Unknown message type for profile metadata update. Ignoring the message {}", message);
    }
  }

  private void updateProfileMetadata(EntityId entityId, MetadataMessage message) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        for (NamespaceMeta meta : namespaceMDS.list()) {
          updateProfileMetadata(meta.getNamespaceId(), message);
        }
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        for (ApplicationMeta meta :  appMetadataStore.getAllApplications(namespaceId.getNamespace())) {
          updateAppProfileMetadata(namespaceId.app(meta.getId()), meta.getSpec());
        }
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        ApplicationMeta meta = appMetadataStore.getApplication(appId.getNamespace(), appId.getApplication(),
                                                               appId.getVersion());
        if (meta == null) {
          LOG.warn("Application {} is not found, so its profile metadata will not get updated. " +
                     "Ignoring the message {}", appId, message);
          return;
        }
        updateAppProfileMetadata(appId, meta.getSpec());
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        break;
      case SCHEDULE:
        ScheduleId scheduleId = (ScheduleId) entityId;
        break;
      default:
        // this should not happen
        LOG.warn("Type of the entity id {} cannot be used to update profile metadata. " +
                   "Ignoring the message {}", entityId, message);
    }
  }

  private void removeProfileMetadata(EntityId entityId, Set<EntityId> entityIds) {

  }

  private void updateAppProfileMetadata(ApplicationId applicationId, ApplicationSpecification appSpec) {

  }
}
