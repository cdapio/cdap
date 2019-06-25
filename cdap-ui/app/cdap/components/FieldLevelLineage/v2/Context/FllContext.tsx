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
import {
  parseRelations,
  makeTargetFields,
  IField,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';

const FllContext = React.createContext({});

function getFieldsAndLinks(d) {
  const incoming = parseRelations(d.entityId.namespace, d.entityId.dataset, d.incoming);
  const outgoing = parseRelations(d.entityId.namespace, d.entityId.dataset, d.outgoing, false);
  const causeTables = incoming.tables;
  const impactTables = outgoing.tables;
  const links = incoming.relLinks.concat(outgoing.relLinks);
  return { causeTables, impactTables, links };
}

export function Provider({ children }) {
  const parsedRes = getFieldsAndLinks(data);

  const defaultState = {
    target: data.entityId.dataset,
    targetFields: makeTargetFields(data.entityId, data.fields) as IField[],
    links: parsedRes.links,
    causeSets: parsedRes.causeTables,
    impactSets: parsedRes.impactTables,
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
