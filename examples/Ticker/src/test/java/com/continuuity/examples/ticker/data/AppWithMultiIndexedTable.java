/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.ticker.data;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.procedure.AbstractProcedure;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 *
 */
public class AppWithMultiIndexedTable extends AbstractApplication {

  @Override
  public void configure() {
    Set<byte[]> ignoredFields = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    setName("AppWithMultiIndexedTable");
    setDescription("Simple app with indexed table dataset");
    createDataset("indexedTable", MultiIndexedTable.class,
                  MultiIndexedTable.properties(Bytes.toBytes("ts"), ignoredFields));
    addProcedure(new AbstractProcedure("fooProcedure") {
    });
  }
}
