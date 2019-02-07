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

package co.cask.cdap.data2.sql;

import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/**
 * A tempory class to register the specification of the SQL tables created.
 * TODO: CDAP-14673 convert this into actual schema registry
 */
public class SqlStructuredTableRegistry implements StructuredTableRegistry {
  private static final Map<StructuredTableId, StructuredTableSpecification> specMap = new ConcurrentHashMap<>();

  @Override
  public void initialize() {
    // Nothing to do
  }

  public void registerSpecification(StructuredTableSpecification specification) {
    specMap.put(specification.getTableId(), specification);
  }

  @Nullable
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return specMap.get(tableId);
  }

  public void removeSpecification(StructuredTableId tableId) {
    specMap.remove(tableId);
  }

  public boolean isEmpty() {
    return specMap.isEmpty();
  }
}
