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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.Updatable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link DatasetAdmin} that propagates administrative operations to the given list of
 * {@link co.cask.cdap.api.dataset.DatasetAdmin}s
 */
@Beta
public class CompositeDatasetAdmin implements DatasetAdmin, Updatable {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeDatasetAdmin.class);

  protected final Map<String, DatasetAdmin> delegates;

  /**
   * Constructor that takes list of dataset admins
   * @param admins list of dataset admins
   */
  public CompositeDatasetAdmin(Map<String, ? extends DatasetAdmin> admins) {
    if (admins == null) {
      throw new IllegalArgumentException("delegates map must not be null");
    }
    this.delegates = Collections.unmodifiableMap(new HashMap<>(admins));
  }

  @Override
  public boolean exists() throws IOException {
    boolean exists = true;
    for (DatasetAdmin admin : delegates.values()) {
      exists = exists && admin.exists();
    }
    return exists;
  }

  @Override
  public void create() throws IOException {
    for (DatasetAdmin admin : delegates.values()) {
      admin.create();
    }
  }

  @Override
  public void drop() throws IOException {
    for (DatasetAdmin admin : delegates.values()) {
      admin.drop();
    }
  }

  @Override
  public void truncate() throws IOException {
    for (DatasetAdmin admin : delegates.values()) {
      admin.truncate();
    }
  }

  @Override
  public void upgrade() throws IOException {
    for (DatasetAdmin admin : delegates.values()) {
      admin.upgrade();
    }
  }

  @Override
  public void close() throws IOException {
    for (DatasetAdmin admin : delegates.values()) {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.warn("Exception raised when calling close() on {} of type {}", admin, admin.getClass(), e);
      }
    }
  }

  @Override
  public void update(DatasetSpecification oldSpec) throws IOException {
    for (Map.Entry<String, DatasetAdmin> entry : delegates.entrySet()) {
      DatasetAdmin admin = entry.getValue();
      if (admin instanceof Updatable) {
        ((Updatable) admin).update(oldSpec.getSpecification(entry.getKey()));
      }
    }
  }
}
