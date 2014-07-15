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

package com.continuuity.api.data.dataset.table;

/**
 * Table that keeps all data in memory.
 * One of the usage examples is an in-memory cache. Writes/updates made to this table provide transactional guarantees
 * when used. For example, updates made in a flowlet process method that failed will have no effect.
 * 
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.table.MemoryTable}
 */
@Deprecated
public class MemoryTable extends Table {
  public MemoryTable(String name) {
    super(name);
  }
}
