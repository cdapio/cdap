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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Entity representation for Metadata
 */
public class MetadataEntity implements Iterable<MetadataEntity.KeyValue> {

  public static final String NAMESPACE = "namespace";
  public static final String APPLICATION = "application";
  public static final String ARTIFACT = "artifact";
  public static final String VERSION = "version";
  public static final String DATASET = "dataset";
  public static final String STREAM = "stream";
  public static final String VIEW = "view";
  public static final String TYPE = "type";
  public static final String PROGRAM = "program";

  private final LinkedHashMap<String, String> details;
  private String type;

  private MetadataEntity() {
    this.details = new LinkedHashMap<>();
  }

  private MetadataEntity(MetadataEntity metadataEntity) {
    this.details = new LinkedHashMap<>(metadataEntity.details);
    this.type = metadataEntity.type;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName. To create a {@link MetadataEntity} for a
   * dataset in a specified namespace please use {@link MetadataEntity#ofDataset(String, String)}.
   *
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String datasetName) {
    MetadataEntity metadataEntity = new MetadataEntity();
    metadataEntity.details.put(MetadataEntity.DATASET, datasetName);
    metadataEntity.type = MetadataEntity.DATASET;
    return metadataEntity;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName in the specified namespace.
   *
   * @param namespace the name of the namespace
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String namespace, String datasetName) {
    MetadataEntity metadataEntity = new MetadataEntity();
    metadataEntity.details.put(MetadataEntity.NAMESPACE, namespace);
    metadataEntity.details.put(MetadataEntity.DATASET, datasetName);
    metadataEntity.type = MetadataEntity.DATASET;
    return metadataEntity;
  }

  /**
   * Creates a {@link MetadataEntity} representing the given namespace.
   *
   * @param namespace the name of the namespace
   * @return {@link MetadataEntity} representing the namespace name
   */
  public static MetadataEntity ofNamespace(String namespace) {
    MetadataEntity metadataEntity = new MetadataEntity();
    metadataEntity.details.put(MetadataEntity.NAMESPACE, namespace);
    metadataEntity.type = MetadataEntity.NAMESPACE;
    return metadataEntity;
  }

  /**
   * Creates a new {@link MetadataEntity} which consists of the given key and values following the key and values of
   * this {@link MetadataEntity}
   *
   * @param key the key to be added
   * @param value the value to be added
   * @return a new {@link MetadataEntity} which consists of the given key and values following the key and values of
   * this {@link MetadataEntity}
   */
  public MetadataEntity append(String key, String value) {
    MetadataEntity metadataEntity = new MetadataEntity(this);
    this.details.put(key, value);
    this.type = key;
    return metadataEntity;
  }

  public String getType() {
    return type;
  }

  @Nullable
  public String getValue(String key) {
    return details.get(key);
  }

  public Iterable<String> getValues() {
    return Collections.unmodifiableList(new ArrayList<>(details.values()));
  }

  public Iterable<String> getKeys() {
    return Collections.unmodifiableList(new ArrayList<>(details.keySet()));
  }

  /**
   * @return A {@link List} of {@link KeyValue} representing the metadata entity
   */
  @Override
  public Iterator<KeyValue> iterator() {
    List<KeyValue> result = new LinkedList<>();
    details.forEach((s, s2) -> result.add(new KeyValue(s, s2)));
    return result.stream().iterator();
  }

  /**
   * {@link MetadataEntity} key-value.
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
