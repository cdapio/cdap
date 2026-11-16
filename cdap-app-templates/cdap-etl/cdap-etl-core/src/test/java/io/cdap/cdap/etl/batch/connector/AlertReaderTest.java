/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.connector;

import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.common.Constants;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AlertReader}.
 */
public class AlertReaderTest {

  private static final Gson GSON = new Gson();

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testReadAlertsSkipsSuccessAndHiddenFiles() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location baseLocation = locationFactory.create("alerts");
    Location phase1 = baseLocation.append(Constants.Connector.DATA_DIR).append("phase-1");
    Location phase2 = baseLocation.append(Constants.Connector.DATA_DIR).append("phase-2");
    phase1.mkdirs();
    phase2.mkdirs();

    Map<String, String> payload1 = Collections.singletonMap("k1", "v1");
    Map<String, String> payload2 = Collections.singletonMap("k2", "v2");
    Alert alert1 = new Alert("stage1", payload1);
    Alert alert2 = new Alert("stage2", payload2);

    phase1.append("_SUCCESS").createNew();
    phase1.append(".part-m-00000.crc").createNew();
    try (Writer writer = new OutputStreamWriter(
        phase1.append("part-m-00000").getOutputStream(), StandardCharsets.UTF_8)) {
      writer.write(GSON.toJson(alert1) + "\n");
    }
    try (Writer writer = new OutputStreamWriter(
        phase2.append("part-m-00000").getOutputStream(), StandardCharsets.UTF_8)) {
      writer.write(GSON.toJson(alert2) + "\n");
    }

    FileSet fileSet = Mockito.mock(FileSet.class);
    Mockito.when(fileSet.getBaseLocation()).thenReturn(baseLocation);

    Map<String, Alert> readByStage = new HashMap<>();
    try (AlertReader reader = new AlertReader(fileSet)) {
      while (reader.hasNext()) {
        Alert next = reader.next();
        readByStage.put(next.getStageName(), next);
      }
    }

    Assert.assertEquals(2, readByStage.size());
    Assert.assertEquals(alert1, readByStage.get("stage1"));
    Assert.assertEquals(alert2, readByStage.get("stage2"));
  }

  @Test
  public void testComputeNextPropagatesIoException() throws Exception {
    IOException readFailure = new IOException("simulated read failure");
    InputStream failingReadStream = new InputStream() {
      @Override
      public int read() throws IOException {
        throw readFailure;
      }
    };

    FileSet fileSet = createMockFileSetWithStream(failingReadStream);
    AlertReader reader = new AlertReader(fileSet);
    try {
      reader.hasNext();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException e) {
      Assert.assertSame(readFailure, e.getCause());
    }
  }

  @Test
  public void testClosePropagatesIoException() throws Exception {
    IOException closeFailure = new IOException("simulated close failure");
    InputStream failingCloseStream = new InputStream() {
      @Override
      public int read() {
        return -1;
      }

      @Override
      public void close() throws IOException {
        throw closeFailure;
      }
    };

    FileSet fileSet = createMockFileSetWithStream(failingCloseStream);
    AlertReader reader = new AlertReader(fileSet);
    try {
      reader.close();
      Assert.fail("Expected RuntimeException wrapping IOException on close");
    } catch (RuntimeException e) {
      Assert.assertSame(closeFailure, e.getCause());
    }
  }

  private FileSet createMockFileSetWithStream(InputStream inputStream) throws IOException {
    Location fileLoc = Mockito.mock(Location.class);
    Mockito.when(fileLoc.getName()).thenReturn("part-00000");
    Mockito.when(fileLoc.getInputStream()).thenReturn(inputStream);

    Location dirLoc = Mockito.mock(Location.class);
    List<Location> files = new ArrayList<>();
    files.add(fileLoc);
    Mockito.when(dirLoc.list()).thenReturn(files);

    Location dataLoc = Mockito.mock(Location.class);
    List<Location> dirs = new ArrayList<>();
    dirs.add(dirLoc);
    Mockito.when(dataLoc.list()).thenReturn(dirs);

    Location baseLoc = Mockito.mock(Location.class);
    Mockito.when(baseLoc.append(Constants.Connector.DATA_DIR)).thenReturn(dataLoc);

    FileSet fileSet = Mockito.mock(FileSet.class);
    Mockito.when(fileSet.getBaseLocation()).thenReturn(baseLoc);
    return fileSet;
  }
}
