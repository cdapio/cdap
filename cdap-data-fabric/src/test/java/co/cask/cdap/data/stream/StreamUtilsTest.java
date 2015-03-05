/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link StreamUtils}.
 */
public class StreamUtilsTest {

  @Test
  public void testValidPartition() {
    Assert.assertTrue(StreamUtils.isPartition("00012345.00345"));
    Assert.assertTrue(StreamUtils.isPartition("1.1"));

    Assert.assertFalse(StreamUtils.isPartition(".1"));
    Assert.assertFalse(StreamUtils.isPartition("123."));

    Assert.assertFalse(StreamUtils.isPartition("12345"));
    Assert.assertFalse(StreamUtils.isPartition("abc.123"));
  }

  @Test
  public void testStreamIdFromLocation() {
    LocationFactory locationFactory = new LocalLocationFactory();
    String path = "/cdap/default/streams/fooStream";
    Location streamBaseLocation = locationFactory.create(path);
    Id.Stream expectedId = Id.Stream.from("default", "fooStream");
    Assert.assertEquals(expectedId, StreamUtils.getStreamIdFromLocation(streamBaseLocation));


    path = "/cdap/othernamespace/streams/otherstream";
    streamBaseLocation = locationFactory.create(path);
    expectedId = Id.Stream.from("othernamespace", "otherstream");
    Assert.assertEquals(expectedId, StreamUtils.getStreamIdFromLocation(streamBaseLocation));
  }

  @Test
  public void testGetStateStoreTableName() {
    Id.Namespace namespace = Id.Namespace.from("foonamespace");
    String expected = "foonamespace.system.stream.state.store";
    Assert.assertEquals(expected, StreamUtils.getStateStoreTableName(namespace));
  }

  @Test
  public void testGetStateStoreTableId() {
    Id.Namespace namespace = Id.Namespace.from("foonamespace");
    TableId expected = TableId.from("foonamespace", "system.stream.state.store");
    Assert.assertEquals(expected, StreamUtils.getStateStoreTableId(namespace));
  }
}
