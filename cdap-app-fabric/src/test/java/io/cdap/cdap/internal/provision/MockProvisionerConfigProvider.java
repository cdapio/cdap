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

package co.cask.cdap.internal.provision;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *  Provides provisioners config for unit test
 */
public class MockProvisionerConfigProvider implements ProvisionerConfigProvider {

  @Override
  public Map<String, ProvisionerConfig> loadProvisionerConfigs(Set<String> provisioners) {
    // Only need to return an empty map since the provisioning service will take care of provisioner that does
    // not have a config
    return Collections.emptyMap();
  }
}
