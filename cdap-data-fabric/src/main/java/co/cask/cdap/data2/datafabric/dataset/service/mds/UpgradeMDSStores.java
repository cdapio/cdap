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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

/**
 * Class for supporting upgrade on {@link DatasetInstanceMDS} and {@link DatasetTypeMDS} from
 * {@link DatasetInstanceMDSUpgrader} and {@link DatasetTypeMDSUpgrader}. This class stores the old and new
 * table together and allows to iterate over them in a transactional.
 */
final class UpgradeMDSStores<T> implements Iterable<T> {
  private final List<T> stores;

  UpgradeMDSStores(T oldMds, T newMds) {
    this.stores = ImmutableList.of(oldMds, newMds);
  }

  protected T getOldMds() {
    return stores.get(0);
  }

  protected T getNewMds() {
    return stores.get(1);
  }

  @Override
  public Iterator<T> iterator() {
    return stores.iterator();
  }
}
