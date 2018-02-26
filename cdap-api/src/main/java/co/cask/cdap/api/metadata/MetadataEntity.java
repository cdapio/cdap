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
package co.cask.cdap.api.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Entity representation for Metadata
 */
public class MetadataEntity {

  public static final String NAMESPACE = "namespace";
  public static final String APPLICATION = "application";
  public static final String ARTIFACT = "artifact";
  public static final String VERSION = "version";
  public static final String DATASET = "dataset";
  public static final String STREAM = "stream";
  public static final String VIEW = "view";
  public static final String KERBEROS_PRINCIPAL = "namespace";
  public static final String TYPE = "type";
  public static final String PROGRAM = "namespace";

  private final List<KeyValue> details;

  private MetadataEntity(List<KeyValue> details) {
    this.details = details;
  }

  /**
   * Creates a {@link MetadataEntity} containing the given dsName. Note: This {@link MetadataEntity} is not
   * equivalent represent of a DatasetId and does not represent a dataset information completely. The namespace to
   * which the dataset belongs must be added to the returned {@link MetadataEntity} by the client. Alternatively, you
   * can also construct a {@link MetadataEntity} which is equivalent to DatasetId by providing the namespace using
   * {@link MetadataEntity#ofDataset(String, String)}.
   *
   * @param dsName the name of the dataset
   * @return {@link MetadataEntity} containing the dataset name
   */
  public static MetadataEntity ofDataset(String dsName) {
    return new MetadataEntity(Collections.singletonList(new KeyValue("dataset", dsName)));
  }

  /**
   * Creates a {@link MetadataEntity} containing the given dsName. Note: This {@link MetadataEntity} is not
   * equivalent represent of a DatasetId and does not represent a dataset information completely. The namespace to
   * which the dataset belongs must be added to the returned {@link MetadataEntity} by the client.
   *
   * @param namespace the name of the namespace
   * @param dsName the name of the dataset
   * @return {@link MetadataEntity} containing the dataset name
   */
  public static MetadataEntity ofDataset(String namespace, String dsName) {
    return new MetadataEntity(Arrays.asList(new KeyValue(MetadataEntity.NAMESPACE, namespace),
                                            new KeyValue(MetadataEntity.DATASET, dsName)));
  }

  /**
   * Creates a {@link MetadataEntity} containing the given namespace.
   *
   * @param ns the name of the namespace
   * @return {@link MetadataEntity} containing the namespace name
   */
  public static MetadataEntity ofNamespace(String ns) {
    return new MetadataEntity(Collections.singletonList(new KeyValue("namespace", ns)));
  }

  /**
   * Adds the given key and value to the existing {@link MetadataEntity}
   *
   * @param key the key to be added
   * @param value the value to be added
   */
  public MetadataEntity of(String key, String value) {
    details.add(new KeyValue(key, value));
    return this;
  }

  /**
   * @return A {@link List} of {@link KeyValue} of representing the the metadata entity
   */
  public List<KeyValue> getDetails() {
    return details;
  }

  /**
   * {@link MetadataEntity} key-value
   */
  public static class KeyValue {
    private final String key;
    private final String value;

    public KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }
}
