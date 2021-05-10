/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.connection;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Response for a sample request
 */
public class SampleResponse {
  private final ConnectorSpec spec;
  // schema for the sample, this is a separate field since we don't want to serialize each record with schema.
  // if the sample is empty, schema is null
  private final Schema schema;
  private final List<StructuredRecord> sample;

  public SampleResponse(ConnectorSpec spec, @Nullable Schema schema, List<StructuredRecord> sample) {
    this.spec = spec;
    this.schema = schema;
    this.sample = sample;
  }

  public ConnectorSpec getSpec() {
    return spec;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public List<StructuredRecord> getSample() {
    return sample;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SampleResponse that = (SampleResponse) o;
    return Objects.equals(spec, that.spec) &&
             Objects.equals(schema, that.schema) &&
             Objects.equals(sample, that.sample);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spec, schema, sample);
  }
}
