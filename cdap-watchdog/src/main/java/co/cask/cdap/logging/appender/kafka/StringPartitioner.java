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

package co.cask.cdap.logging.appender.kafka;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.LoggingConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A simple partitioner based on String keys.
 */
@SuppressWarnings("UnusedDeclaration")
public final class StringPartitioner implements Partitioner<String> {
  private final int numPartitions;

  public StringPartitioner(VerifiableProperties props) {
    this.numPartitions = Integer.parseInt(props.getProperty(LoggingConfiguration.NUM_PARTITIONS));
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be at least 1. Got %s", this.numPartitions);
  }

  @Inject
  public StringPartitioner(CConfiguration cConf) {
    this.numPartitions = cConf.getInt(LoggingConfiguration.NUM_PARTITIONS, -1);
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be greater than 0. Got numPartitions=%s", this.numPartitions);
  }

  @Override
  public int partition(String key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key).asInt()) % this.numPartitions;
  }
}
