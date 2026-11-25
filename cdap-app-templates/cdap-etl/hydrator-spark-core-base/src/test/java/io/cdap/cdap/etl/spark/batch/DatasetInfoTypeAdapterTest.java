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

package io.cdap.cdap.etl.spark.batch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.data.batch.Split;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DatasetInfoTypeAdapter}.
 */
public class DatasetInfoTypeAdapterTest {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
      .create();

  /**
   * Test split implementation for Gson round-trip serialization.
   */
  public static class SampleSplit extends Split {
    private final String splitId;

    public SampleSplit(String splitId) {
      this.splitId = splitId;
    }

    public String getSplitId() {
      return splitId;
    }
  }

  @Test
  public void testSerializeAndDeserializeWithContextAndFallbackClassLoader() {
    Map<String, String> args = Collections.singletonMap("k", "v");
    List<Split> splits = Collections.singletonList(new SampleSplit("s1"));
    DatasetInfo original = new DatasetInfo("myDataset", args, splits);

    String json = GSON.toJson(original);
    DatasetInfo deserialized = GSON.fromJson(json, DatasetInfo.class);
    Assert.assertEquals("myDataset", deserialized.getDatasetName());
    Assert.assertEquals(args, deserialized.getDatasetArgs());
    Assert.assertNotNull(deserialized.getSplits());
    Assert.assertEquals(1, deserialized.getSplits().size());
    Assert.assertEquals("s1", ((SampleSplit) deserialized.getSplits().get(0)).getSplitId());

    ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    try {
      DatasetInfo fallbackDeserialized = GSON.fromJson(json, DatasetInfo.class);
      Assert.assertEquals("myDataset", fallbackDeserialized.getDatasetName());
      Assert.assertNotNull(fallbackDeserialized.getSplits());
      Assert.assertEquals("s1",
          ((SampleSplit) fallbackDeserialized.getSplits().get(0)).getSplitId());
    } finally {
      Thread.currentThread().setContextClassLoader(previousLoader);
    }
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeUnknownSplitClassThrowsJsonParseException() {
    String badJson = "{\"datasetName\":\"ds\",\"datasetArgs\":{},"
        + "\"datasetSplitClass\":\"non.existent.SplitClass\",\"datasetSplits\":[]}";
    GSON.fromJson(badJson, DatasetInfo.class);
  }
}
