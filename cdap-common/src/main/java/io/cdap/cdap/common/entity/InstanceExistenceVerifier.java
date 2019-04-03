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

package co.cask.cdap.common.entity;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.InstanceId;
import com.google.inject.Inject;

/**
 * {@link EntityExistenceVerifier} for {@link InstanceId}.
 */
public class InstanceExistenceVerifier implements EntityExistenceVerifier<InstanceId> {
  private final String instanceName;

  @Inject
  InstanceExistenceVerifier(CConfiguration cConf) {
    instanceName = cConf.get(Constants.INSTANCE_NAME);
  }

  @Override
  public void ensureExists(InstanceId instanceId) throws NotFoundException {
    if (!instanceName.equals(instanceId.getInstance())) {
      throw new NotFoundException(instanceId);
    }
  }
}
