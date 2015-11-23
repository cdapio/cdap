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

package co.cask.cdap.etl.batch.source;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests to verify configuration of {@link FileBatchSource}
 */
public class FileBatchSourceTest {
  @Test
  public void testDefaults() {
    FileBatchSource.FileBatchConfig config = new FileBatchSource.FileBatchConfig();
    FileBatchSource fileBatchSource = new FileBatchSource(config);
    FileBatchSource.FileBatchConfig fileBatchConfig = fileBatchSource.getConfig();
    Assert.assertEquals(new Gson().toJson(ImmutableMap.<String, String>of()), fileBatchConfig.fileSystemProperties);
    Assert.assertEquals(".*", fileBatchConfig.fileRegex);
    Assert.assertEquals(CombineTextInputFormat.class.getName(), fileBatchConfig.inputFormatClass);
    Assert.assertNotNull(fileBatchConfig.maxSplitSize);
    Assert.assertEquals(FileBatchSource.DEFAULT_MAX_SPLIT_SIZE, (long) fileBatchConfig.maxSplitSize);
  }
}
