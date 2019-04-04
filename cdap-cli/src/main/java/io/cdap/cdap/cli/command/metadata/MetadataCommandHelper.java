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
package io.cdap.cdap.cli.command.metadata;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.id.EntityId;

/**
 * Helper for CLI to convert string representation of {@link EntityId} used in metadata CLI command
 * to/from {@link MetadataEntity}
 */
public class MetadataCommandHelper {
  private static final String METADATA_ENTITY_KV_SEPARATOR = "=";
  private static final String METADATA_ENTITY_PARTS_SEPARATOR = ",";
  private static final String METADATA_ENTITY_TYPE = "type";

  /**
   * Returns a CLI friendly representation of MetadataEntity.
   * For a dataset ds1 in ns1 the EntityId representation will be
   * <pre>dataset:ns1.ds1</pre>
   * It's equivalent MetadataEntity representation will be
   * <pre>MetadataEntity{details={namespace=ns1, dataset=ds1}, type='dataset'}</pre>
   * The CLI friendly representation will be
   * <pre>namespace=ns1,dataset=ds1,type=dataset</pre>
   *
   * Note: It is not necessary to give a type, if a type is not provided the
   * last key-value pair's key in the hierarchy will be considered as the type.
   */
  public static String toCliString(MetadataEntity metadataEntity) {
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

  /**
   * Converts a CLI friendly string representation of MetadataEntity to MetadataEntity. For more details see
   * documentation for {@link #toCliString(MetadataEntity)}
   *
   * @param cliString the cli friendly string representation
   * @return {@link MetadataEntity}
   */
  public static MetadataEntity toMetadataEntity(String cliString) {
    MetadataEntity metadataEntity;
    try {
      // For backward compatibility we support entityId.toString representation from CLI for metadata for example
      // dataset representation look likes dataset:namespaceName.datasetName. Try to parse it as CDAP entity if it
      // fails then take the representation to be MetadataEntity which was introduced in CDAP 5.0
      metadataEntity = EntityId.fromString(cliString).toMetadataEntity();
    } catch (IllegalArgumentException e) {
      metadataEntity = fromCliString(cliString);
    }
    return metadataEntity;
  }

  private static MetadataEntity fromCliString(String cliString) {
    String[] keyValues = cliString.split(METADATA_ENTITY_PARTS_SEPARATOR);
    int lastKeyValueIndex = keyValues.length - 1;
    MetadataEntity.Builder builder = MetadataEntity.builder();

    String customType = null;
    if (getKeyValue(keyValues[lastKeyValueIndex])[0].equalsIgnoreCase(METADATA_ENTITY_TYPE)) {
      // if a type is specified then store it to call appendAsType later
      customType = getKeyValue(keyValues[lastKeyValueIndex])[1];
      lastKeyValueIndex -= 1;
    }

    for (int i = 0; i <= lastKeyValueIndex; i++) {
      String[] part = getKeyValue(keyValues[i]);
      if (part[0].equals(customType)) {
        builder.appendAsType(part[0], part[1]);
      } else {
        builder.append(part[0], part[1]);
      }
    }
    return builder.build();
  }

  private static String[] getKeyValue(String keyValue) {
    String[] keyValuePair = keyValue.split(METADATA_ENTITY_KV_SEPARATOR);
    if (keyValuePair.length != 2 || keyValuePair[0].isEmpty() || keyValuePair[1].isEmpty()) {
      throw new IllegalArgumentException(String.format("%s is an invalid key-value.", keyValue));
    }
    return keyValuePair;
  }
}
