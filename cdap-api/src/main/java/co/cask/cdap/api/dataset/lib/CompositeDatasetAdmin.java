/*
 * Copyright © 2014 Cask Data, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link DatasetAdmin} that propagates administrative operations to the given list of
 * {@link co.cask.cdap.api.dataset.DatasetAdmin}s
 */
@Beta
public class CompositeDatasetAdmin implements DatasetAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeDatasetAdmin.class);

  private final List<DatasetAdmin> delegates;

  /**
   * Constructor that takes list of dataset admins
   * @param admins list of dataset admins
   */
  public CompositeDatasetAdmin(Collection<? extends DatasetAdmin> admins) {
    this.delegates = Collections.unmodifiableList(new ArrayList<>(admins));
  }

  /**
   * Constructor that takes list of dataset admins
   * @param admins list of dataset admins
   */
  public CompositeDatasetAdmin(DatasetAdmin... admins) {
    this(Arrays.asList(admins));
  }

  @Override
  public boolean exists() throws IOException {
    boolean exists = true;
    for (DatasetAdmin admin : delegates) {
      exists = exists && admin.exists();
    }
    return exists;
  }

  @Override
  public void create() throws IOException {
    for (DatasetAdmin admin : delegates) {
      admin.create();
    }
  }

  @Override
  public void drop() throws IOException {
    for (DatasetAdmin admin : delegates) {
      admin.drop();
    }
  }

  @Override
  public void truncate() throws IOException {
    for (DatasetAdmin admin : delegates) {
      admin.truncate();
    }
  }

  @Override
  public void upgrade() throws IOException {
    for (DatasetAdmin admin : delegates) {
      admin.upgrade();
    }
  }

  @Override
  public void close() throws IOException {
    for (DatasetAdmin admin : delegates) {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.warn("Exception raised when calling close() on {} of type {}", admin, admin.getClass(), e);
      }
    }
  }
}
