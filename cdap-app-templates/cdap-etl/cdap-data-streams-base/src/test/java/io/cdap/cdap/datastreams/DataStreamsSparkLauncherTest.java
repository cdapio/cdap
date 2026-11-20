/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.datastreams;

import io.cdap.cdap.api.Resources;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link DataStreamsSparkLauncher}.
 */
public class DataStreamsSparkLauncherTest {

  @Test
  public void testComputeMaxRemoteBlockSizeFetchToMemBoundedFor2048MBExecutors() {
    long actual = DataStreamsSparkLauncher.computeMaxRemoteBlockSizeFetchToMem(
      new Resources(2048), new Resources(2048));
    Assert.assertEquals(1024L * 1024L * 1024L, actual);
    Assert.assertTrue(actual <= 1610612736L);
  }

  @Test
  public void testComputeMaxRemoteBlockSizeFetchToMemUsesSmallerOfDriverAndExecutor() {
    long actual = DataStreamsSparkLauncher.computeMaxRemoteBlockSizeFetchToMem(
      new Resources(3072), new Resources(2048));
    Assert.assertEquals(1024L * 1024L * 1024L, actual);
  }

  @Test
  public void testComputeMaxRemoteBlockSizeFetchToMemCapsAtMaxIntMinus512ForLargeContainers() {
    long actual = DataStreamsSparkLauncher.computeMaxRemoteBlockSizeFetchToMem(
      new Resources(4096), new Resources(4096));
    Assert.assertEquals((long) Integer.MAX_VALUE - 512L, actual);
  }

  @Test
  public void testComputeMaxRemoteBlockSizeFetchToMemHandlesNullResources() {
    long actual = DataStreamsSparkLauncher.computeMaxRemoteBlockSizeFetchToMem(null, null);
    Assert.assertEquals(1024L * 1024L * 1024L, actual);
  }
}
