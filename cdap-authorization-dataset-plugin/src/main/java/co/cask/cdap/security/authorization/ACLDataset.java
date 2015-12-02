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

import java.util.HashSet;
import java.util.Set;

/**
 * System dataset for storing ACLs for authorization.
 */
class ACLDataset extends AbstractDataset {

  public static final String NAME = "acls";
  public static final Id.DatasetInstance ID = Id.DatasetInstance.from(Id.Namespace.SYSTEM, NAME);

  private static final byte[] VALUE_COLUMN = new byte[0];

  private final Table table;

  ACLDataset(Table table) {
    super(NAME, table);
    this.table = table;
  }

  /**
   * @param entity the entity
   * @param user the user
   * @return the set of actions allowed for the user on the entity
   */
  public Set<Action> search(EntityId entity, String user) {
    Set<Action> result = new HashSet<>();

    MDSKey mdsKey = getKey(entity, user);
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

  public void add(EntityId entity, String user, Action action) {
    table.put(getKey(entity, user, action).getKey(), VALUE_COLUMN, Bytes.toBytes(action.name()));
  }

  public void remove(EntityId entity, String user, Action action) {
    table.delete(getKey(entity, user, action).getKey());
  }

  public void remove(EntityId entity, String user) {
    MDSKey mdsKey = getKey(entity, user);
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
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(entity.toString());
    return builder.build();
  }

  private MDSKey getKey(EntityId entity, String user) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(entity.toString());
    builder.add(user);
    return builder.build();
  }

  private MDSKey getKey(EntityId entity, String user, Action action) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(entity.toString());
    builder.add(user);
    builder.add(action.name());
    return builder.build();
  }
}
