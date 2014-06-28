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
package com.continuuity.examples.counttokens;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.lib.KeyValueTable;

/**
 * CountTokens application contains a Flow {@code CountTokens} and is attached
 * to a Stream named "text".  It utilizes a KeyValueTable to persist data.
 */
public class CountTokens extends AbstractApplication {

  public static final String TABLE_NAME = "tokenCountTable";

  @Override
  public void configure() {
    setName("CountTokens");
    setDescription("Example applicaiton that counts tokens");
    addStream(new Stream("text"));
    createDataset(TABLE_NAME, KeyValueTable.class);
    addFlow(new CountTokensFlow());
  }
}
