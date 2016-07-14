/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api.dataset.lib.cask360;

import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupType;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataTime;

import com.google.gson.JsonElement;

import java.util.Map;

/**
 * Common interface that links core class {@link Cask360GroupData} with
 * underlying implementations like {@link Cask360GroupDataMap} and
 * {@link Cask360GroupDataTime}.
 */
public interface Cask360GroupDataSpec {

  Cask360GroupType getType();

  Map<byte[], byte[]> getBytesMap(byte[] prefix);

  void put(Cask360GroupData data);

  void put(byte[] column, byte[] value);

  void readJson(JsonElement json);

  JsonElement toJson();

  @Override
  String toString();
}
