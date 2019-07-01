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

export interface IField {
  id: string;
  type: string;
  name: string;
  dataset: string;
  namespace: string;
}

export interface ILink {
  source: IField;
  destination: IField;
}

export interface ITableFields {
  [tablename: string]: IField[];
}

// Parses an incoming or outgoing entity object from backend response to get array of edges and an object with fields keyed by dataset
// namespace and target are the target namespace and dataset name
export function parseRelations(namespace, target, ents, isCause = true) {
  const tables: ITableFields = {};
  const relLinks = [];
  ents.map((ent) => {
    // Assumes that all tableNames are unique (since all datasets in a namespace should have unique names)

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
      const field: IField = {
        id,
        type: isCause ? 'cause' : 'impact',
        name: fieldName,
        dataset: ent.entityId.dataset,
        namespace: ent.entityId.namespace,
      };
      if (!fieldIds.has(fieldName)) {
        id = `${tableId}_fd-${fieldName}`;
        field.id = id;
        fieldIds.set(fieldName, id);
        tables[tableId].push(field);
      }
      const targetField: IField = {
        id: `target_ns-${namespace}_ds-${target}_fd-${isCause ? rel.destination : rel.source}`,
        type: 'target',
        dataset: target,
        namespace,
        name: isCause ? rel.destination : rel.source,
      };
      const link: ILink = {
        source: isCause ? field : targetField,
        destination: isCause ? targetField : field,
      };
      relLinks.push(link);
    });
  });
  return { tables, relLinks };
}

export function makeTargetFields(entityId, fields) {
  const targetFields = fields.map((fieldname) => {
    const id = `target_ns-${entityId.namespace}_ds-${entityId.dataset}_fd-${fieldname}`;
    const field: IField = {
      id,
      type: 'target',
      name: fieldname,
      dataset: entityId.dataset,
      namespace: entityId.namespace,
    };
    return field;
  });
  return targetFields;
}
