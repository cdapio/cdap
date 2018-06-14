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

package co.cask.cdap.proto.id;

import co.cask.cdap.api.metadata.MetadataEntity;

import java.util.List;

/**
 *
 */
public class CustomEntityId extends EntityId {
  private List<MetadataEntity.KeyValue> subParts;
  private String customType;

  public CustomEntityId(EntityId parent, MetadataEntity metadataEntity) {
    super(parent.getEntityType());
    this.subParts = metadataEntity.getFrom(parent.getEntityType().toString());
    this.customType = metadataEntity.getType();
  }

  public List<MetadataEntity.KeyValue> getSubParts() {
    return subParts;
  }

  public String getCustomType() {
    return customType;
  }

  @Override
  public Iterable<String> toIdParts() {
    throw new UnsupportedOperationException("toIdParts is not supported for custom entity type");
  }

  @Override
  public String getEntityName() {
    return subParts.get(subParts.size() - 1).getValue();
  }
}
