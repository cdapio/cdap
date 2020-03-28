/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.inject.Inject;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;

/**
 * Fetch preferences locally via {@link PreferencesService}
 */
public class LocalPreferencesFetcherInternal implements PreferencesFetcher {

  private final PreferencesService preferencesService;

  @Inject
  public LocalPreferencesFetcherInternal(PreferencesService preferencesService) {
    this.preferencesService = preferencesService;
  }

  /**
   * Get preferences for the given identify
   */
  public PreferencesDetail get(EntityId entityId, boolean resolved) {
    final PreferencesService service = preferencesService;
    PreferencesDetail detail = null;
    switch (entityId.getEntityType()) {
      case INSTANCE:
        detail = resolved ? service.getResolvedPreferences() : service.getPreferences();
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        detail = resolved ? service.getResolvedPreferences(namespaceId) : service.getPreferences(namespaceId);
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        detail = resolved ? service.getResolvedPreferences(appId) : service.getPreferences(appId);
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        detail = resolved ? service.getResolvedPreferences(programId) : service.getPreferences(programId);
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
    return detail;
  }
}

