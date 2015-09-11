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

package co.cask.cdap.data.view;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.io.SQLSchemaParser;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test the {@link ViewStore} implementations.
 */
public abstract class ViewStoreTestBase {

  protected abstract ViewStore getExploreViewStore();

  @Test
  public void testExploreViewStore() throws Exception {
    ViewStore store = getExploreViewStore();

    Id.Stream stream = Id.Stream.from("foo", "s");
    Id.Stream.View view1 = Id.Stream.View.from(stream, "bar1");
    Id.Stream.View view2 = Id.Stream.View.from(stream, "bar2");
    Id.Stream.View view3 = Id.Stream.View.from(stream, "bar3");

    Assert.assertFalse(store.exists(view1));

    ViewSpecification properties = new ViewSpecification(
      new FormatSpecification("a", createSchema("name string, props map<string, string>")));
    Assert.assertTrue("view1 should be created", store.createOrUpdate(view1, properties));
    Assert.assertTrue("view1 should exist", store.exists(view1));
    Assert.assertEquals("view1 should have the initial properties",
                        properties, new ViewSpecification(store.get(view1)));

    ViewSpecification properties2 = new ViewSpecification(
      new FormatSpecification("b", createSchema("name string, age int")));
    Assert.assertFalse("view1 should be updated", store.createOrUpdate(view1, properties2));
    Assert.assertTrue("view1 should exist", store.exists(view1));
    Assert.assertEquals("view1 should have the updated properties",
                        properties2, new ViewSpecification(store.get(view1)));

    Assert.assertTrue("view2 should be created", store.createOrUpdate(view2, properties));
    Assert.assertTrue("view3 should be created", store.createOrUpdate(view3, properties));
    Assert.assertEquals("view1, view2, and view3 should be in the stream",
                        ImmutableSet.of(view1, view2, view3), ImmutableSet.copyOf(store.list(stream)));

    store.delete(view1);
    Assert.assertFalse(store.exists(view1));

    store.delete(view2);
    Assert.assertFalse(store.exists(view2));

    store.delete(view3);
    Assert.assertFalse(store.exists(view3));

    Assert.assertEquals(ImmutableList.of(), store.list(stream));
  }

  private Schema createSchema(String sqlSchema) throws IOException {
    return new SQLSchemaParser(sqlSchema).parse();
  }
}
