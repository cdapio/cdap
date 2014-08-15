/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.security.auth;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Abstract base class for {@link KeyManager} implementations that store all secret keys in an in-memory Map.
 */
public abstract class MapBackedKeyManager extends AbstractKeyManager {
  protected final Map<Integer, KeyIdentifier> allKeys = Maps.newConcurrentMap();

  public MapBackedKeyManager(CConfiguration cConf) {
    super(cConf);
  }

  @Override
  protected boolean hasKey(int id) {
    return allKeys.containsKey(id);
  }

  @Override
  protected KeyIdentifier getKey(int id) {
    return allKeys.get(id);
  }

  @Override
  protected void addKey(KeyIdentifier key) {
    allKeys.put(key.getKeyId(), key);
  }
}
