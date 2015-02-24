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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.proto.Id;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Helper methods for writing Stream file tests.
 */
public class StreamFileTestUtils {

  private StreamFileTestUtils() {
  }

  public static Location getStreamBaseLocation(LocationFactory locationFactory,
                                               Id.Stream streamId) throws IOException {
    return locationFactory.create(streamId.getNamespaceId()).append(streamId.getName());
  }

  public static StreamEvent createEvent(long timestamp, String body) {
    return new StreamEvent(ImmutableMap.<String, String>of(), Charsets.UTF_8.encode(body), timestamp);
  }

  public static Location createTempDir(LocationFactory locationFactory) {
    try {
      Location dir = locationFactory.create("/").getTempFile(".dir");
      dir.mkdirs();
      return dir;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
