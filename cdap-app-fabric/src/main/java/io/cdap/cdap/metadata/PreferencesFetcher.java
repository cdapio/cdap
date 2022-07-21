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

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;

/**
 * Interface for fetching {@code PreferencesDetail}
 */
public interface PreferencesFetcher {
  /**
   * Get preferences for the given identify
   * @param entityId the id of the entity to fetch preferences for
   * @param resolved true if resolved properties are desired.
   * @return the detail of preferences
   * @throws IOException if failed to get preferences
   * @throws NotFoundException if the given entity doesn't exist.
   */
  PreferencesDetail get(EntityId entityId, boolean resolved)
    throws IOException, NotFoundException, UnauthorizedException;
}
