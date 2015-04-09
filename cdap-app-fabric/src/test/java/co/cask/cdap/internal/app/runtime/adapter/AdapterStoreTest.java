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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.RunIds;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AdapterStore}.
 */
public class AdapterStoreTest extends AppFabricTestBase {

  @Test
  public void testRunIds() throws Exception {
    AdapterStore store = getInjector().getInstance(AdapterStore.class);
    Id.Adapter adapter1 = Id.Adapter.from("def", "someAdapter");
    Assert.assertNull(store.getRunId(adapter1));
    RunId id = RunIds.generate();
    store.setRunId(adapter1, id);
    Assert.assertEquals(id, store.getRunId(adapter1));
    store.deleteRunId(adapter1);
    Assert.assertNull(store.getRunId(adapter1));
  }
}
