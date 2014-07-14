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

package com.continuuity.api.dataset.lib;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.dataset.DatasetAdmin;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of {@link DatasetAdmin} that propagates administrative operations to the given list of
 * {@link com.continuuity.api.dataset.DatasetAdmin}s
 */
@Beta
public class CompositeDatasetAdmin implements DatasetAdmin {
  private final List<DatasetAdmin> delegates;

  /**
   * Constructor that takes list of dataset admins
   * @param admins list of dataset admins
   */
  public CompositeDatasetAdmin(Collection<? extends DatasetAdmin> admins) {
    this.delegates = ImmutableList.copyOf(admins);
  }

  /**
   * Constructor that takes list of dataset admins
   * @param admins list of dataset admins
   */
  public CompositeDatasetAdmin(DatasetAdmin... admins) {
    this.delegates = ImmutableList.copyOf(admins);
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
      Closeables.closeQuietly(admin);
    }
  }
}
