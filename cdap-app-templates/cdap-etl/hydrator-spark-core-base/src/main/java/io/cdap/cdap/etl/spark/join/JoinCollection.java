/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.join;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.spark.SparkCollection;

import java.util.List;

/**
 * Data to join.
 */
public class JoinCollection {
  private final String stage;
  private final String type;
  private final SparkCollection<?> data;
  private final Schema schema;
  private final List<String> key;
  private final boolean broadcast;

  public JoinCollection(String stage, String type, SparkCollection<?> data, Schema schema,
                        List<String> key, boolean broadcast) {
    this.stage = stage;
    this.type = type;
    this.data = data;
    this.schema = schema;
    this.key = key;
    this.broadcast = broadcast;
  }

  public String getStage() {
    return stage;
  }

  public String getType() {
    return type;
  }

  public SparkCollection<?> getData() {
    return data;
  }

  public Schema getSchema() {
    return schema;
  }

  public List<String> getKey() {
    return key;
  }

  public boolean isBroadcast() {
    return broadcast;
  }
}
