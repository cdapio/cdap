/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.ScopedName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import static co.cask.cdap.api.metadata.MetadataScope.SYSTEM;
import static co.cask.cdap.api.metadata.MetadataScope.USER;

public class MetadataCodecTest {

  @Test
  public void testSerialization() {

    Gson gson = new GsonBuilder().registerTypeAdapter(Metadata.class, new MetadataCodec()).setPrettyPrinting().create();

    MetadataRecord record = new MetadataRecord(MetadataEntity.ofDataset("myns", "myds"),
                                               new Metadata(ImmutableSet.of(new ScopedName(SYSTEM, "a"),
                                                                            new ScopedName(SYSTEM, "b"),
                                                                            new ScopedName(USER, "a"),
                                                                            new ScopedName(SYSTEM, "c")),
                                                            ImmutableMap.of(new ScopedName(SYSTEM, "x"), "1",
                                                                            new ScopedName(SYSTEM, "y"), "2",
                                                                            new ScopedName(USER, "x"), "3",
                                                                            new ScopedName(USER, "z"), "4")));

    String json = gson.toJson(record);
    Assert.assertEquals(record, gson.fromJson(json, MetadataRecord.class));

    // without the type adapter, GSon would serialize a ScopedName as "scope:name".
    // Hence it would contain the String "SYSTEM:x". Validate that it does not.
    Assert.assertFalse(json.contains(SYSTEM.name() + ":"));
  }
}
