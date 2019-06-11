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
    const tableName = ent.entityId.dataset;
    // tables keeps track of fields for each incoming or outgoing dataset
    tables[tableName] = [];
    // fieldIds keeps track of fields we've seen already, since a single field can have multiple connections
    const fieldIds = new Map();
    ent.relations.map((rel) => {
      // backend response assumes connection goes from left to right, i.e. an incoming connection's source = incoming field and destination = target field, and outgoing connection's source = target field.
      const fieldName = isCause ? rel.source : rel.destination;
      let id = fieldIds.get(fieldName);
      const field = {
        id,
        name: fieldName,
        group: tableName,
      };
      if (!fieldIds.has(fieldName)) {
        id = `${namespace}_${tableName}_${fieldName}`;
        field.id = id;
        fieldIds.set(fieldName, id);
        tables[tableName].push(field);
        relNodes.push(field);
      }
      relLinks.push({
        source: isCause ? fieldIds.get(fieldName) : `${namespace}_${target}_${rel.source}`,
        destination: isCause
          ? `${namespace}_${target}_${rel.destination}`
          : fieldIds.get(fieldName),
      });
    });
  });
  return { tables, relNodes, relLinks };
}

export function makeTargetNodes(entityId, fields) {
  const targetNodes = fields.map((field) => {
    const id = `${entityId.namespace}_${entityId.dataset}_${field}`;
    return {
      id,
      name: field,
      group: entityId.dataset,
    };
  });
  return targetNodes;
}
