/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.app.store.PreviewStore;
import co.cask.cdap.proto.id.ProgramId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Default implementation of the {@link PreviewStore} that stores data in in-memory table
 */
public class InMemoryPreviewStore implements PreviewStore {

  private Map<ProgramId, Map<String, List<Object>>> store = new HashMap<>();

  @Override
  public synchronized void put(ProgramId programId, String propertyName, Object value) {

  }

  @Override
  public Map<String, List<Object>> get(ProgramId programId) {
    return null;
  }

  @Override
  public void remove(ProgramId programId) {

  }

  @Override
  public void clear() {

  }
}
