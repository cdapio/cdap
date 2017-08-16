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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.batch.Output;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alianwar on 8/16/17.
 */
public class OutputCodecTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Output.class, new OutputCodec<>())
    .create();
  private static final Type OUTPUT_LIST_TYPE = new TypeToken<List<Output>>() { }.getType();

  @Test
  public void test() {
    List<Output> objects = new ArrayList<>();
    objects.add(Output.ofDataset("ds"));
    GSON.toJson(objects, OUTPUT_LIST_TYPE);
    String dsName = GSON.toJson(Output.ofDataset("dsName"), Output.class);
    Output output = GSON.fromJson(dsName, Output.class);
  }
}
