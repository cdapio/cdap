/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.authorization;

import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.InMemoryACLStore;
import co.cask.common.authorization.client.ACLStoreSupplier;
import com.google.inject.Singleton;

/**
 * Provides {@link co.cask.cdap.data2.dataset2.lib.table.ACLStoreTable}.
 */
@Singleton
public class InMemoryACLStoreTableSupplier implements ACLStoreSupplier {

  private static final InMemoryACLStore aclStore = new InMemoryACLStore();

  @Override
  public ACLStore get() {
    return aclStore;
  }
}
