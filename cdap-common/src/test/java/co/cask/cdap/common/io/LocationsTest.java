/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Unit test for {@link Locations}
 */
public class LocationsTest {
  private static final String TEST_BASE_PATH = "test_base";
  private static final String TEST_PATH = "some/test/path";

  @Test
  public void absolutePathTests() throws IOException {
    // Test HDFS:
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://1.2.3.4:8020/");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, TEST_BASE_PATH);
    Location location1 = locationFactory.create(TEST_PATH);
    Location location2 = Locations.getLocationFromAbsolutePath(locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());

    // Test file:
    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    locationFactory = new FileContextLocationFactory(conf, TEST_BASE_PATH);
    location1 = locationFactory.create(TEST_PATH);
    location2 = Locations.getLocationFromAbsolutePath(locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());

    // Test LocalLocation
    locationFactory = new LocalLocationFactory(new File(TEST_BASE_PATH));
    location1 = locationFactory.create(TEST_PATH);
    location2 = Locations.getLocationFromAbsolutePath(locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());
  }
}


