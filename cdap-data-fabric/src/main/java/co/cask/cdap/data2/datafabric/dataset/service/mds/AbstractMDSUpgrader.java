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

import co.cask.cdap.common.io.Locations;
import com.google.common.collect.ImmutableList;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * An Abstract class which provides common methods and classes need by Dataset MDS to be upgraded
 */
public abstract class AbstractMDSUpgrader {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMDSUpgrader.class);


  /**
   * An abstract method which should be implemented by dataset MDS which needs to be upgraded
   * @throws Exception
   */
  public abstract void upgrade() throws Exception;

  /**
   * Renames the old location to new location if old location exists and the new one does not
   *
   * @param oldLocation the old {@link Location}
   * @param newLocation the new {@link Location}
   * @return new location if and only if the file or directory is successfully moved; null otherwise.
   * @throws IOException
   */
  @Nullable
  protected Location renameLocation(Location oldLocation, Location newLocation) throws IOException {
    // if the newLocation does not exists or the oldLocation does we try to rename. If either one of them is false then
    // the underlying call to renameTo will throw IOException which we propagate.
    if (!newLocation.exists() || oldLocation.exists()) {
      Locations.getParent(newLocation).mkdirs();
      try {
        return oldLocation.renameTo(newLocation);
      } catch (IOException ioe) {
        newLocation.delete();
        LOG.warn("Failed to rename {} to {}", oldLocation, newLocation);
        throw ioe;
      }
    } else {
      LOG.debug("New location {} already exists and old location {} does not exists. The location might already be " +
                  "updated.", newLocation, oldLocation);
      return null;
    }
  }

  /**
   * Class for supporting upgrade on {@link DatasetInstanceMDS} and {@link DatasetTypeMDS} from
   * {@link DatasetInstanceMDSUpgrader} and {@link DatasetTypeMDSUpgrader}. This class stores the old and new
   * table together and allows to iterate over them in a transactional.
   */
  protected static final class UpgradeMDSStores<T> implements Iterable<T> {
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
}
