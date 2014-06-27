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
package com.continuuity.examples.countandfilterwords;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.lib.KeyValueTable;

/**
 * CountAndFilterWordsDemo application contains a Flow {@code CountAndFilterWords} and is attached
 * to a Stream named "text".
 */
public class CountAndFilterWords extends AbstractApplication {

  public static final String TABLE_NAME = "filterTable";

  @Override
  public void configure() {
    setName("CountAndFilterWords");
    setDescription("Example word filter and count Application");
    addStream(new Stream("text"));
    createDataset(TABLE_NAME, KeyValueTable.class);
    addFlow(new CountAndFilterWordsFlow());
  }
}
