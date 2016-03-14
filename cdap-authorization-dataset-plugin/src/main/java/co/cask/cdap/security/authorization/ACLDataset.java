/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.security.authorization;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;

import java.util.HashSet;
import java.util.Set;

/**
 * System dataset for storing ACLs for authorization.
 */
class ACLDataset extends AbstractDataset {

  private static final String NAME = "acls";
  private static final byte[] VALUE_COLUMN = new byte[0];

  static final Id.DatasetInstance ID = Id.DatasetInstance.from(Id.Namespace.SYSTEM, NAME);

  private final Table table;

  ACLDataset(Table table) {
    super(NAME, table);
    this.table = table;
  }

  /**
   * @param entity the entity
   * @param principal the principal
   * @return the set of actions allowed for the user on the entity
   */
  public Set<Action> search(EntityId entity, Principal principal) {
    Set<Action> result = new HashSet<>();

    MDSKey mdsKey = getKey(entity, principal);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    Scanner scan = table.scan(startKey, stopKey);

    try {
      Row next;
      while ((next = scan.next()) != null) {
        byte[] value = next.get(VALUE_COLUMN);
        if (value == null) {
          continue;
        }
        result.add(Action.valueOf(Bytes.toString(value)));
      }
    } finally {
      scan.close();
    }

    return result;
  }

  public void add(EntityId entity, Principal principal, Action action) {
    table.put(getKey(entity, principal, action).getKey(), VALUE_COLUMN, Bytes.toBytes(action.name()));
  }

  public void remove(EntityId entity, Principal principal, Action action) {
    table.delete(getKey(entity, principal, action).getKey());
  }

  public void remove(EntityId entity, Principal principal) {
    MDSKey mdsKey = getKey(entity, principal);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    Scanner scan = table.scan(startKey, stopKey);

    try {
      Row next;
      while ((next = scan.next()) != null) {
        table.delete(next.getRow());
      }
    } finally {
      scan.close();
    }
  }

  public void remove(EntityId entity) {
    MDSKey mdsKey = getKey(entity);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    Scanner scan = table.scan(startKey, stopKey);

    try {
      Row next;
      while ((next = scan.next()) != null) {
        table.delete(next.getRow());
      }
    } finally {
      scan.close();
    }
  }

  private MDSKey getKey(EntityId entity) {
    return getKeyBuilder(entity).build();
  }

  private MDSKey getKey(EntityId entity, Principal principal) {
    return getKeyBuilder(entity, principal).build();
  }

  private MDSKey getKey(EntityId entity, Principal principal, Action action) {
    return getKeyBuilder(entity, principal, action).build();
  }

  private MDSKey.Builder getKeyBuilder(EntityId entity, Principal principal, Action action) {
    return getKeyBuilder(entity, principal).add(action.name());
  }

  private MDSKey.Builder getKeyBuilder(EntityId entity, Principal principal) {
    return getKeyBuilder(entity).add(principal.getType().name()).add(principal.getName());
  }

  private MDSKey.Builder getKeyBuilder(EntityId entity) {
    return new MDSKey.Builder().add(entity.toString());
  }
}
