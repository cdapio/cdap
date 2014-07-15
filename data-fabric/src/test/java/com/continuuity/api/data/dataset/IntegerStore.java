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

package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.internal.io.UnsupportedTypeException;

/**
 * A simple data set <i>extending</i> ObjectStore, used by ObjectStoreTest.testSubclass().
 */
public class IntegerStore extends ObjectStore<Integer> {

  public IntegerStore(String name) throws UnsupportedTypeException {
    super(name, Integer.class);
  }

  public void write(int key, Integer value) {
    super.write(Bytes.toBytes(key), value);
  }

  public Integer read(int key) {
    return super.read(Bytes.toBytes(key));
  }

}
