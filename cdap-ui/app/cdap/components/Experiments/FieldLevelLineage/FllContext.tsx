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

import React from 'react';
import data from './sample_response';
import uuidV4 from 'uuid/v4';

interface INode {
  id: string;
  name: string;
  group: number;
}

const FllContext = React.createContext({});

function makeNodes(fields) {
  const targetNodes = fields.map((field) => {
    const id = uuidV4();
    return {
      id,
      name: field,
      group: data.entityId.dataset,
    };
  });
  return targetNodes;
}

function getFieldsAndLinks(d) {
  const incoming = parseRelations(d.incoming);
  const outgoing = parseRelations(d.outgoing, false);
  const causeSets = incoming.sets;
  const impactSets = outgoing.sets;
  const nodes = incoming.relNodes.concat(outgoing.relNodes);
  const links = incoming.relLinks.concat(outgoing.relLinks);

  function parseRelations(ents, isCause = true) {
    const sets = {};
    const relNodes = [];
    const relLinks = [];
    ents.map((ent) => {
      const tableName = ent.entityId.dataset;
      sets[tableName] = [];
      const fieldIds = new Map();
      ent.relations.map((rel) => {
        const fieldName = isCause ? rel.source : rel.destination;
        let id = fieldIds.get(fieldName);
        const field = {
          id,
          name: fieldName,
          group: tableName,
        };
        if (!fieldIds.has(fieldName)) {
          id = uuidV4();
          field.id = id;
          fieldIds.set(fieldName, id);
          sets[tableName].push(field);
          relNodes.push(field);
        }
        relLinks.push({
          source: isCause ? fieldIds.get(fieldName) : rel.source,
          destination: isCause ? rel.destination : fieldIds.get(fieldName),
        });
      });
    });
    return { sets, relNodes, relLinks };
  }
  return { causeSets, impactSets, nodes, links };
}

export function Provider({ children }) {
  const defaultState = {
    target: data.entityId.dataset,
    targetFields: makeNodes(data.fields) as INode[],
    nodes: getFieldsAndLinks(data).nodes,
    links: getFieldsAndLinks(data).links,
    causeSets: getFieldsAndLinks(data).causeSets,
    impactSets: getFieldsAndLinks(data).impactSets,
    activeField: null,
    numTables: 4,
    firstCause: 1,
    firstImpact: 1,
    firstField: 1,
  };

  return <FllContext.Provider value={defaultState}>{children}</FllContext.Provider>;
}

export function Consumer({ children }) {
  return <FllContext.Consumer>{children}</FllContext.Consumer>;
}
