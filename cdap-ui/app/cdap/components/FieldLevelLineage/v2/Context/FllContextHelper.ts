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

// Parses an incoming or outgoing entity object from backend response to get unique nodes (one per incoming or outgoing field), connections, and an object with fieldnames for each incoming or outgoing dataset

export function parseRelations(namespace, target, ents, isCause = true) {
  const tables = {};
  const relNodes = [];
  const relLinks = [];
  ents.map((ent) => {
    // Assumes that all tableNames are unique (since all datasets in a namespace should have unique names)
    // tableName is later used to display the table name in the header

    const tableId = `${isCause ? 'cause' : 'impact'}_ns-${ent.entityId.namespace}_ds-${
      ent.entityId.dataset
    }`;
    // tables keeps track of fields for each incoming or outgoing dataset.
    tables[tableId] = [];
    // fieldIds keeps track of fields we've seen already, since a single field can have multiple connections
    const fieldIds = new Map();
    ent.relations.map((rel) => {
      // backend response assumes connection goes from left to right, i.e. an incoming connection's source = incoming field and destination = target field, and outgoing connection's source = target field.
      const fieldName = isCause ? rel.source : rel.destination;
      let id = fieldIds.get(fieldName);
      const field = {
        id,
        name: fieldName,
        group: ent.entityId.dataset,
      };
      if (!fieldIds.has(fieldName)) {
        id = `${tableId}_fd-${fieldName}`;
        field.id = id;
        fieldIds.set(fieldName, id);
        tables[tableId].push(field);
        relNodes.push(field);
      }
      relLinks.push({
        // if connection goes from cause to target
        source: isCause
          ? fieldIds.get(fieldName)
          : `target_ns-${namespace}_ds-${target}_fd-${rel.source}`,
        destination: isCause
          ? `target_ns-${namespace}_ds-${target}_fd-${rel.destination}`
          : fieldIds.get(fieldName),
      });
    });
  });
  return { tables, relNodes, relLinks };
}

export function makeTargetNodes(entityId, fields) {
  const targetNodes = fields.map((field) => {
    const id = `target_ns-${entityId.namespace}_ds-${entityId.dataset}_fd-${field}`;
    return {
      id,
      name: field,
      group: entityId.dataset,
    };
  });
  return targetNodes;
}
