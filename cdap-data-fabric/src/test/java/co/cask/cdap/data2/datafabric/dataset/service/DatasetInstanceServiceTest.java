/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;

public class DatasetInstanceServiceTest extends DatasetServiceTestBase {

  @BeforeClass
  public static void setup() throws Exception {
    DatasetServiceTestBase.initialize();
  }

  @Test
  public void testInstanceMetaCache() throws Exception {

    // deploy a dataset
    instanceService.create(Id.Namespace.DEFAULT.getId(), "testds",
                           new DatasetInstanceConfiguration("table", new HashMap<String, String>()));

    // get the dataset meta for two different owners, assert it is the same
    DatasetMeta meta = instanceService.get(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"),
                                           ImmutableList.<Id>of(Id.Flow.from("app1", "flow1")));
    DatasetMeta met2 = instanceService.get(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"),
                                           ImmutableList.<Id>of(Id.Flow.from("app2", "flow2")));
    Assert.assertSame(meta, met2);

    // update the dataset
    instanceService.update(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"),
                           ImmutableMap.of("ttl", "12345678"));

    // get the dataset meta, validate it changed
    met2 = instanceService.get(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"),
                               ImmutableList.<Id>of(Id.Flow.from("app2", "flow2")));
    Assert.assertNotSame(meta, met2);
    Assert.assertEquals("12345678", met2.getSpec().getProperty("ttl"));

    // delete the dataset
    instanceService.drop(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"));

    // get the dataset meta, validate not found
    try {
      instanceService.get(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"),
                          ImmutableList.<Id>of(Id.Flow.from("app1", "flow2")));
      Assert.fail("get() should have thrown NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }

    // recreate the dataset
    instanceService.create(Id.Namespace.DEFAULT.getId(), "testds",
                           new DatasetInstanceConfiguration("table", new HashMap<String, String>()));

    // get the dataset meta, validate it is up to date
    met2 = instanceService.get(Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testds"),
                               ImmutableList.<Id>of(Id.Flow.from("app2", "flow2")));
    Assert.assertEquals(meta.getSpec(), met2.getSpec());
  }

}
