/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.kafka;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.nio.charset.Charset;

/**
 * A simple partitioner based on String keys.
 */
@SuppressWarnings("UnusedDeclaration")
public final class StringPartitioner implements Partitioner {
  private final int numPartitions;

  public StringPartitioner(VerifiableProperties props) {
    this.numPartitions = Integer.parseInt(props.getProperty(Constants.Logging.NUM_PARTITIONS));
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be at least 1. Got %s", this.numPartitions);
  }

  @Inject
  public StringPartitioner(CConfiguration cConf) {
    this.numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be greater than 0. Got numPartitions=%s", this.numPartitions);
  }

  @Override
  public int partition(Object key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key.toString(), Charset.defaultCharset()).asInt()) % this.numPartitions;
  }
}
