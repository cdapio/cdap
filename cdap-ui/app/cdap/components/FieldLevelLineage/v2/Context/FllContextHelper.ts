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

import { TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';
import { TIME_OPTIONS_MAP } from 'components/FieldLevelLineage/store/ActionCreator';
import { parseQueryString } from 'services/helpers';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyMetadataApi } from 'api/metadata';
import { Theme } from 'services/ThemeHelper';
import { IContextState, ITimeType } from 'components/FieldLevelLineage/v2/Context/FllContext';
import T from 'i18n-react';

// types for backend response
interface IFllEntity {
  entityId?: IEntityId;
  relations?: IResLink[];
  fieldCount?: number;
}

interface IEntityId {
  namespace?: string;
  dataset?: string;
  entity?: string;
}

interface IOperationsErrorObj {
  statusCode: number;
  response: string;
}
export type IOperationErrorResponse = IOperationsErrorObj | string;

// Info we get from backend about lineage connection
export interface IResLink {
  source?: string;
  destination?: string;
}

// These types are used by the frontend
export interface IField {
  id: string;
  name: string;
  type?: string;
  dataset?: string;
  namespace?: string;
  hasIncomingOps?: boolean;
  hasOutgoingOps?: boolean;
}

// IResLink plus extra info for rendering connections
export interface ILink {
  source: IField;
  destination: IField;
}

export interface ILinkSet {
  incoming: ILink[];
  outgoing: ILink[];
}

// Field count and field name info for all incoming or outgoing datsets
export interface ITablesList {
  [tablename: string]: ITableInfo;
}

export interface ITableInfo {
  fields?: IField[];
  fieldCount?: number;
  unrelatedFields?: IField[];
  isExpanded?: boolean;
  namespace?: string;
  dataset?: string;
}

export interface ITimeParams {
  selection: string;
  range: ITimeRange;
}

interface ITimeRange {
  start: string | number;
  end: string | number;
}

export interface IQueryParams {
  time?: string;
  field?: string;
  start?: string;
  end?: string;
}

export interface IOperationSummary {
  operations: IOperation[];
  programs: IProgram[];
}

interface IProgram {
  lastExecutedTimeInSeconds?: number;
  program: {
    application: string;
    entity: string;
    namespace: string;
    program: string;
    type: string;
    version: string;
  };
}

interface IOperation {
  description: string;
  inputs?: { endpoint: { name: string; namespace: string } };
  name: string;
  outputs?: { fields: string[] };
}

export const getDefaultLinks = () => {
  return { incoming: [], outgoing: [] };
};

/** Parses an incoming or outgoing entity object from backend response
 * to get array of edges and an object with fields keyed by dataset.
 * namespace and target are the target namespace and dataset name
 */

export function getLinks(
  namespace: string,
  target: string,
  ents: IFllEntity[],
  isCause: boolean = true
) {
  const tables: ITablesList = {};
  const links: ILink[] = [];
  const targetFieldsWithOps = new Set(); // Keep track of all target fields with operations/lineage

  ents.forEach((ent) => {
    // Assumes that all tableNames are unique within a namespace

    // Check if entity should not be rendered (i.e. from dropped or added field)
    if (!ent.hasOwnProperty('entityId')) {
      // Just mark the target field(s) as having operations, for the dangling links
      ent.relations.map((rel) => {
        const targetField = isCause ? rel.destination : rel.source; // Grab the target field
        targetFieldsWithOps.add(targetField);
      });
      return;
    }

    const type = isCause ? 'cause' : 'impact';
    const tableId = getTableId(ent.entityId.dataset, ent.entityId.namespace, type);

    // tables keeps track of datasets and fields that need to be rendered.
    tables[tableId] = {
      fieldCount: ent.fieldCount,
      fields: [],
      dataset: ent.entityId.dataset,
      namespace: ent.entityId.namespace,
    };
    // fieldIds keeps track of fields to prevent duplication, since a single field can have multiple connections
    const fieldIds = new Map();

    // Go through all the entity's relations to grab fields to be rendered and the target fields (which have ops)
    ent.relations.map((rel) => {
      /** backend response assumes connection goes from left to right
       * i.e. an incoming connection's destination = target field,
       * and outgoing connection's source = target field.
       */

      const fieldName = isCause ? rel.source : rel.destination;
      let id = fieldIds.get(fieldName);
      const field: IField = {
        id,
        type,
        name: fieldName,
        dataset: ent.entityId.dataset,
        namespace: ent.entityId.namespace,
      };
      // if we haven't seen this field yet, add it to the table's list of fields
      if (!fieldIds.has(fieldName)) {
        id = getFieldId(fieldName, ent.entityId.dataset, ent.entityId.namespace, type);
        field.id = id;
        fieldIds.set(fieldName, id);
        tables[tableId].fields.push(field);
      }

      const targetName = isCause ? rel.destination : rel.source;

      targetFieldsWithOps.add(targetName);

      const targetField: IField = {
        id: getFieldId(targetName, target, namespace, 'target'),
        type: 'target',
        dataset: target,
        namespace,
        name: isCause ? rel.destination : rel.source,
      };
      const link: ILink = {
        source: isCause ? field : targetField,
        destination: isCause ? targetField : field,
      };
      links.push(link);
    });
  });
  return { tables, links, targetFieldsWithOps };
}

export function getFieldsAndLinks(d) {
  const incomingLineage = getLinks(d.entityId.namespace, d.entityId.dataset, d.incoming);
  const outgoingLineage = getLinks(d.entityId.namespace, d.entityId.dataset, d.outgoing, false);
  const causeTables = incomingLineage.tables;
  const impactTables = outgoingLineage.tables;
  const targetFieldsWithOps = {
    incoming: incomingLineage.targetFieldsWithOps,
    outgoing: outgoingLineage.targetFieldsWithOps,
  };
  const links: ILinkSet = { incoming: incomingLineage.links, outgoing: outgoingLineage.links };
  return { causeTables, impactTables, links, targetFieldsWithOps };
}

export function makeTargetFields(
  { namespace, dataset }: IEntityId,
  fields: string[],
  fieldsWithOps
) {
  const targetFields = fields.map((fieldname) => {
    const id = `target_ns-${namespace}_ds-${dataset}_fd-${fieldname}`;
    const hasIncomingOps = fieldsWithOps.incoming.has(fieldname);
    const hasOutgoingOps = fieldsWithOps.outgoing.has(fieldname);
    const field: IField = {
      id,
      type: 'target',
      name: fieldname,
      dataset,
      namespace,
      hasIncomingOps,
      hasOutgoingOps,
    };
    return field;
  });
  return { fields: targetFields };
}

export function getFieldId(fieldname, dataset, namespace, type) {
  return `${type}_ns-${namespace}_ds-${dataset}_fd-${fieldname}`;
}

export function getTableId(dataset, namespace, type) {
  return `${type}_ns-${namespace}_ds-${dataset}`;
}

export function getTimeRangeFromUrl() {
  const queryString = parseQueryString();
  const selection = queryString ? queryString.time : TIME_OPTIONS[1]; // default is last 7 days

  if (selection === TIME_OPTIONS[0]) {
    return {
      selection,
      range: {
        start: queryString.start || 'now-7d',
        end: queryString.end || 'now',
      },
    };
  }
  return { selection, range: TIME_OPTIONS_MAP[selection] };
}

// Currently we are just measuring the size of the response. This will be updated once
// there is proper pagination for the FLL response from backend. This limit is approximately
// 1500 fields going into 1500 fields
function isFLLResponseTooLarge(response) {
  const CHARACTER_LIMIT = 50000;
  const stringifiedResponse = JSON.stringify(response);
  return stringifiedResponse.length > CHARACTER_LIMIT;
}

export function getFieldLineage(
  namespace: string,
  dataset: string,
  qParams: IQueryParams | null,
  timeParams: ITimeParams
) {
  let fieldname: string;
  let activeField: IField;

  if (!qParams || !qParams.field) {
    fieldname = null;
    activeField = { id: null, name: null };
  } else {
    fieldname = qParams.field;
    activeField = {
      name: fieldname,
      id: getFieldId(fieldname, dataset, namespace, 'target'),
    };
  }

  const start = timeParams.range.start;
  const end = timeParams.range.end;

  const params = {
    namespace,
    entityId: dataset,
    direction: 'both',
    start,
    end,
  };

  return MyMetadataApi.getAllFieldLineage(params).map((res) => {
    if (isFLLResponseTooLarge(res)) {
      throw new Error(T.translate('features.FieldLevelLineage.Error.lineageTooLarge').toString());
    }

    const parsedRes = getFieldsAndLinks(res);
    const targetInfo: IContextState = {
      target: res.entityId.dataset,
      targetFields: makeTargetFields(res.entityId, res.fields, parsedRes.targetFieldsWithOps),
      links: parsedRes.links,
      causeSets: parsedRes.causeTables,
      impactSets: parsedRes.impactTables,
      selection: timeParams.selection,
      start,
      end,
      activeField,
      showingOneField: false,
      showOperations: false,
      activeOpsIndex: 0,
      loadingOps: false,
    };

    return targetInfo;
  });
}

function constructQueryParams(
  selection: string,
  activeField: IField,
  start: string | number,
  end: string | number
) {
  const pathname = location.pathname;
  const timeParams = getTimeParamsFromSelection(selection, start, end);
  let url = `${pathname}${timeParams}`;

  if (activeField.id) {
    url = `${url}&field=${activeField.name}`;
  }
  return url;
}

export function getTimeRange(selection) {
  if (TIME_OPTIONS.indexOf(selection) === -1) {
    return;
  }

  let start = null;
  let end = null;

  // set start and end times if not CUSTOM
  if (selection !== TIME_OPTIONS[0]) {
    ({ start, end } = TIME_OPTIONS_MAP[selection]);
  }

  return { start, end };
}

function getTimeParamsFromSelection(
  selection: string,
  start: string | number,
  end: string | number
) {
  let queryParams = `?time=${selection}`;

  if (selection === TIME_OPTIONS[0]) {
    queryParams = `${queryParams}&start=${start}&end=${end}`;
  }
  return queryParams;
}

export function replaceHistory(
  selection: string,
  activeField: IField,
  start: string | number,
  end: string | number
) {
  const url = constructQueryParams(selection, activeField, start, end);
  const currentLocation = location.pathname + location.search;

  if (url === currentLocation) {
    return;
  }

  const stateObj = {
    title: Theme.productName,
    url,
  };

  history.replaceState(stateObj, stateObj.title, stateObj.url);
}

export function getTimeQueryParams(selection, start, end) {
  const timeRange = selection ? selection : TIME_OPTIONS[1];
  let params = `?time=${timeRange}`;
  if (start && selection === TIME_OPTIONS[0]) {
    params = `${params}&start=${start}&end=${end}`;
  }
  return params;
}

export function getOperations(
  dataset: string,
  timeParams: ITimeRange,
  fieldName: string,
  direction: string,
  cb: (lineage: IContextState) => void,
  errcb: (error: IOperationErrorResponse) => void
) {
  const namespace = getCurrentNamespace();
  const params = {
    namespace,
    entityId: dataset,
    fieldName,
    start: timeParams.start,
    end: timeParams.end,
    direction,
  };

  MyMetadataApi.getFieldOperations(params).subscribe((res) => {
    const operations = res[direction];
    cb(operations);
  }, errcb);
}

export function fetchUnrelatedFields(
  namespace: string,
  tablename: string,
  type: string,
  relatedFields: IField[],
  start: ITimeType,
  end: ITimeType
) {
  const isUnrelatedField = (fieldname, fields) => {
    const fieldId = getFieldId(fieldname, tablename, namespace, type);
    return fields.filter((field) => field.id === fieldId).length === 0;
  };

  const getFieldProperties = (fieldname) => {
    const fieldProperties: IField = {
      type,
      namespace,
      name: fieldname,
      dataset: tablename,
      id: getFieldId(fieldname, tablename, namespace, type),
    };
    return fieldProperties;
  };

  const params = { namespace, entityId: tablename, start, end };

  const fieldsObservable = MyMetadataApi.getFields(params).map((res) => {
    // Filter out fields that are already being rendered (to keep field order) and add field properties
    return res
      .filter((fieldname) => isUnrelatedField(fieldname, relatedFields))
      .map((fieldname) => getFieldProperties(fieldname));
  });
  return fieldsObservable;
}
