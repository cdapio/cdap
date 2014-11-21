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

import co.cask.cdap.api.dataset.DatasetProperties;

/**
 * Helper to build properties for files datasets.
 */
public class FileProperties {

  public static Builder builder() {
    return new Builder();
  }

  /**
   * A Builder to construct DatasetProperties instances.
   */
  public static final class Builder {
    private final DatasetProperties.Builder delegate = DatasetProperties.builder();

    private Builder() {
    }

    /**
     * Sets the base path for the file dataset.
     */
    public Builder setBasePath(String path) {
      delegate.add(File.PROPERTY_BASE_PATH, path);
      return this;
    }

    /**
     * Sets the output format of the file dataset.
     */
    public Builder setOutputFormat(Class<?> outputFormatClass) {
      delegate.add(File.PROPERTY_OUTPUT_FORMAT, outputFormatClass.getName());
      return this;
    }

    /**
     * Sets the output format of the file dataset.
     */
    public Builder setInputFormat(Class<?> inputFormatClass) {
      delegate.add(File.PROPERTY_INPUT_FORMAT, inputFormatClass.getName());
      return this;
    }

    /**
     * Sets a property for the input format of the file dataset.
     */
    public Builder setInputProperty(String name, String value) {
      delegate.add(File.PROPERTY_INPUT_PROPERTIES_PREFIX + name, value);
      return this;
    }

    /**
     * Sets a property for the output format of the file dataset.
     */
    public Builder setOutputProperty(String name, String value) {
      delegate.add(File.PROPERTY_OUTPUT_PROPERTIES_PREFIX + name, value);
      return this;
    }

    /**
     * Create a DatasetProperties from this builder.
     */
    public DatasetProperties build() {
      return delegate.build();
    }
  }
}
