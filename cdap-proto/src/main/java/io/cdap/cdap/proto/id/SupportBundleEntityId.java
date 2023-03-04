/*
 * Copyright © 2015-2021 Cask Data, Inc.
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
import java.util.Iterator;

/**
 * Uniquely identifies a support bundle entity.
 */
public class SupportBundleEntityId extends EntityId {

  // the name of the entity
  protected final String supportBundleName;

  public SupportBundleEntityId(String supportBundleName) {
    super(EntityType.SUPPORT_BUNDLE);
    if (supportBundleName == null) {
      throw new NullPointerException("supportBundleName can not be null.");
    }
    this.supportBundleName = supportBundleName;
  }

  @Override
  public String getEntityName() {
    return supportBundleName;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.singletonList(supportBundleName);
  }

  @SuppressWarnings("unused")
  public static SupportBundleEntityId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new SupportBundleEntityId(nextAndEnd(iterator, "supportBundle"));
  }
}

