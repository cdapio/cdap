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
 * the License
 */
package co.cask.cdap.cli.command.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;

/**
 * Helper for CLI to convert string representation to/from {@link MetadataEntity}
 */
public class MetadataCommandHelper {
  private static final String METADATA_ENTITY_KV_SEPARATOR = "=";
  private static final String METADATA_ENTITY_PARTS_SEPARATOR = ",";
  private static final String METADATA_ENTITY_TYPE = "type";

  public static MetadataEntity fromString(String input) {
    String[] keyValues = input.split(METADATA_ENTITY_PARTS_SEPARATOR);
    MetadataEntity metadataEntity = new MetadataEntity();
    boolean customType = false;
    for (int i = 0; i < keyValues.length; i++) {
      String[] part = keyValues[i].split(METADATA_ENTITY_KV_SEPARATOR);
      // if type is specified it should be the last part
      if (i != keyValues.length - 1 && !part[0].equalsIgnoreCase(METADATA_ENTITY_TYPE)) {
        metadataEntity = metadataEntity.append(part[0], part[1]);
      } else {
        customType = true;
      }
    }
    if (customType) {
      metadataEntity = metadataEntity.changeType(keyValues[keyValues.length - 1]
                                                   .split(METADATA_ENTITY_KV_SEPARATOR)[1]);
    }
    return metadataEntity;
  }

  public static String toString(MetadataEntity metadataEntity) {
    StringBuilder builder = new StringBuilder();
    for (MetadataEntity.KeyValue keyValue : metadataEntity) {
      builder.append(keyValue.getKey());
      builder.append(METADATA_ENTITY_KV_SEPARATOR);
      builder.append(keyValue.getValue());
      builder.append(METADATA_ENTITY_PARTS_SEPARATOR);
    }
    builder.append(METADATA_ENTITY_TYPE);
    builder.append(METADATA_ENTITY_KV_SEPARATOR);
    builder.append(metadataEntity.getType());
    return builder.toString();
  }
}
