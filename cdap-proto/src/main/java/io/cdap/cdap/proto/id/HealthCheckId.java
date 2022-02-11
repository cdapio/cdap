/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.proto.id;

import io.cdap.cdap.proto.element.EntityType;

import java.util.Collections;

/**
 * Uniquely identifies a healthCheck.
 */
public class HealthCheckId extends EntityId {
  // the name of the entity
  protected final String healthCheck;

  public HealthCheckId(String healthCheck) {
    super(EntityType.HEALTH_CHECK);
    if (healthCheck == null) {
      throw new NullPointerException("healthCheck can not be null.");
    }
    this.healthCheck = healthCheck;
  }

  @Override
  public String getEntityName() {
    return healthCheck;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.singletonList(healthCheck);
  }
}
