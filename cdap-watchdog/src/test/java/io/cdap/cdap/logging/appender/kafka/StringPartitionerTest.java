/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.Properties;
import kafka.utils.VerifiableProperties;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StringPartitioner} MD5 UTF-16LE deterministic partitioning.
 */
public class StringPartitionerTest {

  @Test
  public void testDeterministicPartitionsForEightAndSixteenPartitions() {
    CConfiguration conf8 = CConfiguration.create();
    conf8.setInt(Constants.Logging.NUM_PARTITIONS, 8);
    StringPartitioner partitioner8 = new StringPartitioner(conf8);

    Properties props16 = new Properties();
    props16.setProperty(Constants.Logging.NUM_PARTITIONS, "16");
    StringPartitioner partitioner16 = new StringPartitioner(new VerifiableProperties(props16));

    // ASCII keys (note: UTF-16LE unencoded chars differ from UTF-8 bytes)
    Assert.assertEquals(1, partitioner8.partition("default:myApp:worker:myWorker", 99));
    Assert.assertEquals(9, partitioner16.partition("default:myApp:worker:myWorker", 99));

    Assert.assertEquals(1, partitioner8.partition("system:services:appfabric", 8));
    Assert.assertEquals(1, partitioner16.partition("system:services:appfabric", 16));

    Assert.assertEquals(3, partitioner8.partition("prod_ns:etl_pipeline:spark:driver", 8));
    Assert.assertEquals(11, partitioner16.partition("prod_ns:etl_pipeline:spark:driver", 16));

    // Non-ASCII and supplementary Unicode keys
    Assert.assertEquals(0, partitioner8.partition("ns_üñîçødé:app:worker:w1", 8));
    Assert.assertEquals(8, partitioner16.partition("ns_üñîçødé:app:worker:w1", 16));

    Assert.assertEquals(7, partitioner8.partition("ns_日本語:app_λ:mapreduce:mr1", 8));
    Assert.assertEquals(15, partitioner16.partition("ns_日本語:app_λ:mapreduce:mr1", 16));

    Assert.assertEquals(3, partitioner8.partition("ns_🚀:app_é:workflow:wf1", 8));
    Assert.assertEquals(11, partitioner16.partition("ns_🚀:app_é:workflow:wf1", 16));
  }

  @Test
  public void testInvalidNumPartitionsRejected() {
    CConfiguration invalidConf = CConfiguration.create();
    invalidConf.setInt(Constants.Logging.NUM_PARTITIONS, 0);
    try {
      new StringPartitioner(invalidConf);
      Assert.fail("Expected IllegalArgumentException for zero partitions");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("numPartitions"));
    }

    Properties invalidProps = new Properties();
    invalidProps.setProperty(Constants.Logging.NUM_PARTITIONS, "-2");
    try {
      new StringPartitioner(new VerifiableProperties(invalidProps));
      Assert.fail("Expected IllegalArgumentException for negative partitions");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("numPartitions"));
    }
  }
}
