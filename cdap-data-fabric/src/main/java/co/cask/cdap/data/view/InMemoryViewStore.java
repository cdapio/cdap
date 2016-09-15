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

package co.cask.cdap.data.view;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory implementation of {@link ViewStore}.
 */
public final class InMemoryViewStore implements ViewStore {

  private final Table<StreamViewId, StreamId, ViewSpecification> views;
  private final ReadWriteLock viewsLock;

  public InMemoryViewStore() {
    this.views = HashBasedTable.create();
    this.viewsLock = new ReentrantReadWriteLock();
  }

  @Override
  public boolean createOrUpdate(StreamViewId viewId, ViewSpecification config) {
    Lock lock = viewsLock.writeLock();
    lock.lock();
    try {
      return views.put(viewId, viewId.getParent(), config) == null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean exists(StreamViewId viewId) {
    Lock lock = viewsLock.readLock();
    lock.lock();
    try {
      return views.contains(viewId, viewId.getParent());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void delete(StreamViewId viewId) throws NotFoundException {
    ViewSpecification removed;
    Lock lock = viewsLock.writeLock();
    lock.lock();
    try {
      removed = views.remove(viewId, viewId.getParent());
    } finally {
      lock.unlock();
    }
    if (removed == null) {
      throw new NotFoundException(viewId);
    }
  }

  @Override
  public List<StreamViewId> list(StreamId streamId) {
    Lock lock = viewsLock.readLock();
    lock.lock();
    try {
      return ImmutableList.<StreamViewId>builder().addAll(views.column(streamId).keySet()).build();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ViewDetail get(StreamViewId viewId) throws NotFoundException {
    Lock lock = viewsLock.readLock();
    lock.lock();
    try {
      if (!views.containsRow(viewId)) {
        throw new NotFoundException(viewId);
      }

      return new ViewDetail(viewId.getEntityName(), views.get(viewId, viewId.getParent()));
    } finally {
      lock.unlock();
    }
  }
}
