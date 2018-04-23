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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

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
  public static final String VIEW = "stream_view";
  public static final String TYPE = "type";
  public static final String PROGRAM = "program";

  private final List<KeyValue> details;

  /**
   * Creates a empty {@link MetadataEntity}
   */
  public MetadataEntity() {
    this.details = Collections.emptyList();
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName. To create a {@link MetadataEntity} for a
   * dataset in a specified namespace please use {@link MetadataEntity#ofDataset(String, String)}.
   *
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String datasetName) {
    return new MetadataEntity(Collections.singletonList(new KeyValue(DATASET, datasetName)));
  }

  private MetadataEntity(List<KeyValue> details) {
    this.details = Collections.unmodifiableList(new ArrayList<>(details));
  }

  /**
   * Creates a {@link MetadataEntity} representing the given datasetName in the specified namespace.
   *
   * @param namespace the name of the namespace
   * @param datasetName the name of the dataset
   * @return {@link MetadataEntity} representing the dataset name
   */
  public static MetadataEntity ofDataset(String namespace, String datasetName) {
    return new MetadataEntity(Arrays.asList(new KeyValue(NAMESPACE, namespace),
                                            new KeyValue(DATASET, datasetName)));
  }

  /**
   * Creates a {@link MetadataEntity} representing the given namespace.
   *
   * @param ns the name of the namespace
   * @return {@link MetadataEntity} representing the namespace name
   */
  public static MetadataEntity ofNamespace(String ns) {
    return new MetadataEntity(Collections.singletonList(new KeyValue(NAMESPACE, ns)));
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
    List<KeyValue> existingParts = new ArrayList<>(getKeyValues());
    existingParts.add(new KeyValue(key, value));
    return new MetadataEntity(existingParts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataEntity that = (MetadataEntity) o;
    return Objects.equals(details, that.details);
  }

  @Override
  public int hashCode() {
    return details.hashCode();
  }

  @Override
  public String toString() {
    return "MetadataEntity{" +
      "details=" + details +
      '}';
  }

  /**
   * @return A {@link List} of {@link KeyValue} representing the metadata entity
   */
  public List<KeyValue> getKeyValues() {
    return details;
  }

  /**
   * @return the value for the key if the key is found else null
   */
  public String getValue(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot be null");
    }
    for (KeyValue detail : details) {
      if (detail.getKey().equals(key)) {
        return detail.getValue();
      }
    }
    return null;
  }

  /**
   * @return A {@link Iterator} over the keys in this {@link MetadataEntity}
   */
  public Iterable<String> getKeys() {
    return details.stream().map(KeyValue::getKey)::iterator;
  }

  /**
   * @return A {@link Iterator} over the values in this {@link MetadataEntity}
   */
  public Iterable<String> getValues() {
    return details.stream().map(KeyValue::getValue)::iterator;
  }

  @Override
  @Nonnull
  public Iterator<KeyValue> iterator() {
    return details.stream().iterator();
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

    @Override
    public String toString() {
      return "KeyValue{" +
        "key='" + key + '\'' +
        ", value='" + value + '\'' +
        '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyValue keyValue = (KeyValue) o;
      return Objects.equals(key, keyValue.key) &&
        Objects.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }
}
