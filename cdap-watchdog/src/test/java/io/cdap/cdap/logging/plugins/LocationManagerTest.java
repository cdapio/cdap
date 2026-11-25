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

package io.cdap.cdap.logging.plugins;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.common.conf.Constants;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link LocationManager}.
 */
public class LocationManagerTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCloseQuietlySwallowsStreamExceptionsAndClosesAllLocations() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    LocationManager manager = new LocationManager(locationFactory, "logs", "700", "600", 0L);

    LocationIdentifier id1 = manager.getLocationIdentifier(ImmutableMap.of(
        Constants.Logging.TAG_NAMESPACE_ID, "ns1",
        Constants.Logging.TAG_APPLICATION_ID, "app1"));
    OutputStream realStream = manager.getLocationOutputStream(id1, "app1.log");
    realStream.write("hello".getBytes(StandardCharsets.UTF_8));
    manager.sync();
    Assert.assertEquals(1, manager.getActiveLocations().size());

    final AtomicBoolean failingFirstClosed = new AtomicBoolean(false);
    final AtomicBoolean secondClosed = new AtomicBoolean(false);
    Location dummyLocation = locationFactory.create("dummy.log");

    LocationOutputStream failingLocationStream = new LocationOutputStream(
        dummyLocation,
        new OutputStream() {
          @Override
          public void write(int b) {
          }

          @Override
          public void close() throws IOException {
            failingFirstClosed.set(true);
            throw new IOException("disk error during close");
          }
        },
        System.currentTimeMillis());

    LocationOutputStream healthyLocationStream = new LocationOutputStream(
        dummyLocation,
        new OutputStream() {
          @Override
          public void write(int b) {
          }

          @Override
          public void close() {
            secondClosed.set(true);
          }
        },
        System.currentTimeMillis());

    manager.getActiveLocations().put(new LocationIdentifier("ns2", "failingApp"),
        failingLocationStream);
    manager.getActiveLocations().put(new LocationIdentifier("ns3", "healthyApp"),
        healthyLocationStream);

    // Must not throw IOException even when one stream fails to close
    manager.close();

    Assert.assertTrue(failingFirstClosed.get());
    Assert.assertTrue(secondClosed.get());
    Assert.assertTrue(manager.getActiveLocations().isEmpty());
  }
}
