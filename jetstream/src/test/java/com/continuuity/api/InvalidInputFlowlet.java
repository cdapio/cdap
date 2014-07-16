/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractInputFlowlet;
import com.continuuity.jetstream.api.StreamSchema;
import com.continuuity.jetstream.api.PrimitiveType;

/**
 * Invalid Flowlet since the Schema has two fields with the same name.
 */
public class InvalidInputFlowlet extends AbstractInputFlowlet {

  @Override
  public void create() {
    setName("invalidFlowlet");
    StreamSchema schema = StreamSchema.Builder.with()
      .field("abcd", PrimitiveType.UINT)
      .field("abcd", PrimitiveType.LLONG)
      .build();
    addGDATInput("invalidInput", schema);
    addGSQL("sql", "SELECT * FROM invalidInput");
  }
}
