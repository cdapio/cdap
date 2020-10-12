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

package io.cdap.cdap.etl.spark;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.counters.AbstractCounter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An abstract MapReduce {@link Counter} for sending counter to Spark metrics.
 */
public abstract class AbstractSparkCounter extends AbstractCounter {

  private final String name;
  private final String displayName;

  public AbstractSparkCounter(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDisplayName() {
    return displayName;
  }

  @Override
  public Counter getUnderlyingCounter() {
    return this;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // no-op
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // no-op
  }
}
