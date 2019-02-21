/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.master.environment;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.master.spi.program.SerDe;
import com.google.gson.Gson;

/**
 * A Gson based SerDe.
 */
public class GsonSerDe implements SerDe {
  private final Gson gson;

  public GsonSerDe(Gson gson) {
    this.gson = gson;
  }

  @Override
  public <T> byte[] serialize(T object) {
    return Bytes.toBytes(gson.toJson(object));
  }

  @Override
  public <T> T deserialize(byte[] bytes, Class<T> objectClass) {
    return gson.fromJson(Bytes.toString(bytes), objectClass);
  }
}
