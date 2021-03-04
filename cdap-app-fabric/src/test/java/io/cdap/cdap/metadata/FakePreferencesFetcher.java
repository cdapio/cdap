/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.EntityId;

import java.util.Map;

/**
 * Fake preferences that just returns whatever it is configured to return.
 */
public class FakePreferencesFetcher implements PreferencesFetcher {
  private final Map<String, String> properties;

  public FakePreferencesFetcher(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public PreferencesDetail get(EntityId entityId, boolean resolved) {
    return new PreferencesDetail(properties, 0L, resolved);
  }
}
