/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetProperties;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Utilities for configuring partitioned dataset properties.
 */
public class PartitionedProperties {

  private static final Gson GSON = new Gson();

  /**
   * The property name for the meta data fields of the partitioned datasets.
   */
  private static final String METADATA_FIELDS = "metadata.fields";
  private static final Type META_FIELDS_TYPE = new TypeToken<Map<String, Partitioned.FieldType>>() { }.getType();

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final DatasetProperties.Builder delegate;

    private Builder() {
      delegate = DatasetProperties.builder();
    }

    public Builder setMetadataFields(Map<String, Partitioned.FieldType> fields) {
      delegate.add(METADATA_FIELDS, GSON.toJson(fields));
      return this;
    }

    public DatasetProperties build() {
      return delegate.build();
    }
  }

  /**
   * Extracts the metadata fields from the dataset properties.
   * @param props the dataset properties for a partitioned dataset
   * @return the metadata fields as deserialized from the properties
   * @throws DataSetException if the metadata fields are not configured in the properties,
   *         or if the property value is ill-formed.
   */
  public static Map<String, Partitioned.FieldType> getMetadataFields(DatasetProperties props)
    throws DataSetException {

    String value = props.getProperties().get(METADATA_FIELDS);

    if (value == null) {
      throw new DataSetException("Dataset properties do not contain property '" + METADATA_FIELDS + "'");
    }

    try {
      return GSON.fromJson(value, META_FIELDS_TYPE);
    } catch (JsonParseException jpe) {
      throw new DataSetException(String.format("Property value for '%s' cannot be parsed: %s",
                                               METADATA_FIELDS, value), jpe);
    }
  }

}
