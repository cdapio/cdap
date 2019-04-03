/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.proto.codec;

import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.proto.BasicThrowable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for BasicThrowableCodec.
 */
public class BasicThrowableCodecTest {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();

  @Test
  public void testCodec() {
    testCodec(new InstanceNotFoundException("myInstance"));
    testCodec(new IllegalArgumentException());
    testCodec(new NullPointerException());
    testCodec(new Exception(new RuntimeException("some error")));
  }

  private void testCodec(Throwable t) {
    BasicThrowable bt = new BasicThrowable(t);
    String json = GSON.toJson(bt);
    BasicThrowable bt1 = GSON.fromJson(json, BasicThrowable.class);
    Assert.assertEquals(bt, bt1);
  }
}
