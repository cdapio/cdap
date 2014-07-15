/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * DataSet Test.
 */
public class DataSetTest extends DataSetTestBase {

  @BeforeClass
  public static void configure() {

    // a key value table
    DataSet table = new KeyValueTable("table");
    // an indexed table
    DataSet index = new IndexedTable("index", Bytes.toBytes("phone"));
    // a data set that lacks the runtime constructor
    DataSet incomplete = new IncompleteDataSet("dummy");
    // a data set that lacks the runtime constructor
    DataSet throwing = new ThrowingDataSet("badguy");

    setupInstantiator(Lists.newArrayList(table, index, incomplete, throwing));
  }

  @Test
  public void testToAndFromJSon() {
    for (DataSetSpecification spec : specs) {
      Gson gson = new Gson();
      String json = gson.toJson(spec);
      //System.out.println("JSON: " + json);
      DataSetSpecification spec1 = gson.fromJson(json, DataSetSpecification.class);
      Assert.assertEquals(spec, spec1);
    }
  }

  @Test(expected = ClassCastException.class)
  public void testInstantiateWrongClass() throws Exception {
    @SuppressWarnings("unused")
    KeyValueTable kvTable = instantiator.getDataSet("index");
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testInstantiateNonExistent() throws Exception {
    @SuppressWarnings("unused")
    KeyValueTable kvTable = instantiator.getDataSet("fhdjkshgjkla");
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testThrowingConstructor() throws Exception {
    // try to instantiate the incomplete data set
    @SuppressWarnings("unused")
    ThrowingDataSet ds = instantiator.getDataSet("badguy");
  }
}
