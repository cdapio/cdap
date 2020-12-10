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
import { objectQuery, parseQueryString, extractErrorMessage } from 'services/helpers';
import { getCurrentNamespace } from 'services/NamespaceStore';
import {
  fetchUnrelatedFields,
  getDefaultLinks,
  getFieldLineage,
  getTableId,
  getTimeRange,
  getTimeRangeFromUrl,
  replaceHistory,
  IField,
  ILinkSet,
  ITableInfo,
  ITablesList,
  ITimeParams,
  IOperationErrorResponse,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import * as d3 from 'd3';
import { TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';
import {
  getOperations,
  IOperationSummary,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import T from 'i18n-react';

const defaultContext: IContextState = {
  target: '',
  targetFields: {},
  links: getDefaultLinks(),
  causeSets: {},
  impactSets: {},
  showingOneField: false,
  start: null,
  end: null,
  selection: TIME_OPTIONS[1],
  showOperations: false,
  activeOpsIndex: 0,
  loadingOps: false,
  loadingLineage: true,
  error: null,
};

export const FllContext = React.createContext<IContextState>(defaultContext);

export type ITimeType = number | string | null;

export interface IContextState {
  target: string;
  targetFields: ITableInfo;
  links: ILinkSet;
  causeSets: ITablesList;
  impactSets: ITablesList;
  showingOneField: boolean;
  start: ITimeType;
  end: ITimeType;
  selection: string;
  showOperations: boolean;
  activeOpsIndex: number;
  loadingOps: boolean;
  loadingLineage?: boolean;
  operations?: IOperationSummary[];
  direction?: string;
  handleFieldClick?: (event: React.MouseEvent<HTMLDivElement>) => void;
  handleViewCauseImpact?: () => void;
  handleReset?: () => void;
  handleExpandFields?: (namespace: string, entityId: string, type: string) => void;
  activeField?: IField;
  activeCauseSets?: ITablesList;
  activeImpactSets?: ITablesList;
  activeLinks?: ILinkSet;
  numTables?: number;
  firstCause?: number;
  firstImpact?: number;
  firstField?: number;
  error?: string;
  errorOps?: string;
  setTimeRange?: (range: string) => void;
  setCustomTimeRange?: ({ start, end }) => void;
  toggleOperations?: (direction?: string) => void;
  prevOperation?: () => void;
  nextOperation?: () => void;
}

export class Provider extends React.Component<{ children }, IContextState> {
  private handleFieldClick = (e: React.MouseEvent<HTMLDivElement>) => {
    const activeFieldId = (e.target as HTMLDivElement).id; // casting because EventTarget lacks target and name
    if (!activeFieldId) {
      return;
    }
    if (this.state.activeField.id) {
      d3.select(`#${this.state.activeField.id}`).classed('selected', false);
    }

    const newField: IField = {
      id: activeFieldId,
      name: (e.target as HTMLDivElement).dataset.fieldname,
    };
    const activeLinks = this.getActiveLinks(activeFieldId);
    const activeSets = this.getActiveSets(activeLinks);

    this.setState(
      {
        activeField: newField,
        activeLinks,
        activeCauseSets: activeSets.activeCauseSets,
        activeImpactSets: activeSets.activeImpactSets,
      },
      () => {
        d3.select(`#${activeFieldId}`).classed('selected', true);
        replaceHistory(
          this.state.selection,
          this.state.activeField,
          this.state.start,
          this.state.end
        );
      }
    );
  };

  private getActiveLinks = (newTargetId?: string, newLinks?: ILinkSet) => {
    const activeFieldId = newTargetId || this.state.activeField.id;
    const activeLinks: ILinkSet = getDefaultLinks();
    const links = newLinks
      ? newLinks.incoming.concat(newLinks.outgoing)
      : this.state.links.incoming.concat(this.state.links.outgoing);
    links.forEach((link) => {
      if (link.destination.id === activeFieldId) {
        activeLinks.incoming.push(link);
      }
      if (link.source.id === activeFieldId) {
        activeLinks.outgoing.push(link);
      }
    });
    return activeLinks;
  };

  private getActiveSets = (activeLinks: ILinkSet = this.state.activeLinks) => {
    const activeCauseSets = {};
    const activeImpactSets = {};

    if (!activeLinks) {
      activeLinks = this.getActiveLinks();
    }

    for (const links of Object.values(activeLinks)) {
      links.forEach((link) => {
        const nonTargetFd = link.source.type !== 'target' ? link.source : link.destination;
        const tableId = getTableId(nonTargetFd.dataset, nonTargetFd.namespace, nonTargetFd.type);

        if (nonTargetFd.type === 'cause') {
          if (!(tableId in activeCauseSets)) {
            activeCauseSets[tableId] = {
              fields: [],
            };
          }
          activeCauseSets[tableId].fields.push(nonTargetFd);
        } else {
          if (!(tableId in activeImpactSets)) {
            activeImpactSets[tableId] = {
              fields: [],
            };
          }
          activeImpactSets[tableId].fields.push(nonTargetFd);
        }
      });
    }

    return { activeCauseSets, activeImpactSets };
  };

  private handleViewCauseImpact = () => {
    this.setState({ showingOneField: true });
  };

  private handleReset = () => {
    this.setState({
      showingOneField: false,
    });
  };

  private fetchFieldLineage(qParams, timeParams, dataset = this.state.target) {
    this.setState({ loadingLineage: true });
    const namespace = getCurrentNamespace();

    let isFetched = false;
    let timeout;

    const fetching$ = getFieldLineage(namespace, dataset, qParams, timeParams).subscribe(
      (newState: IContextState) => {
        isFetched = true;
        clearTimeout(timeout);

        // If no field selected, grab the first field with lineage
        if (!newState.activeField.id && newState.targetFields.fields.length > 0) {
          newState.activeField = {
            id: newState.targetFields.fields[0].id,
            name: newState.targetFields.fields[0].name,
          };
        }

        const activeLinks = this.getActiveLinks(newState.activeField.id, newState.links);
        const activeSets = this.getActiveSets(activeLinks);

        newState.activeLinks = activeLinks;
        newState.activeCauseSets = activeSets.activeCauseSets;
        newState.activeImpactSets = activeSets.activeImpactSets;
        newState.loadingLineage = false;
        newState.error = null;

        this.setState(newState);
      },
      (err) => {
        isFetched = true;
        clearTimeout(timeout);

        const errorMessage =
          err instanceof Error ? err.message : JSON.stringify(extractErrorMessage(err));
        this.setState({
          error: errorMessage,
          target: dataset,
          loadingLineage: false,
        });
      }
    );

    const MAX_FLL_FETCH_TIME = 20000;

    timeout = setTimeout(() => {
      if (isFetched) {
        return;
      }

      fetching$.unsubscribe();
      this.setState({
        error: T.translate('features.FieldLevelLineage.Error.lineageTooLarge').toString(),
        target: dataset,
        loadingLineage: false,
      });
    }, MAX_FLL_FETCH_TIME);
  }

  private updateLineageFromRange(selection: string, start: ITimeType, end: ITimeType) {
    const newState = {
      selection,
      start: null,
      end: null,
      loadingLineage: true,
    };
    // start and end are only set for custom date range
    if (selection === TIME_OPTIONS[0]) {
      newState.start = start;
      newState.end = end;
    }

    this.setState(newState, () => {
      const qParams = parseQueryString();
      const timeParams: ITimeParams = {
        selection,
        range: { start, end },
      };
      this.fetchFieldLineage(qParams, timeParams);

      replaceHistory(
        this.state.selection,
        this.state.activeField,
        this.state.start,
        this.state.end
      );
    });
  }

  private setCustomTimeRange = ({ start, end }: { start: number; end: number }) => {
    this.updateLineageFromRange(TIME_OPTIONS[0], start, end);
  };

  private setTimeRange = (selection: string) => {
    if (TIME_OPTIONS.indexOf(selection) === -1) {
      return;
    }

    const { start, end } = getTimeRange(selection);

    // If CUSTOM, don't update lineage or url until date is picked
    if (selection === TIME_OPTIONS[0]) {
      this.setState({
        selection,
        start: null,
        end: null,
      });
      return;
    }
    this.updateLineageFromRange(selection, start, end);
  };

  private toggleOperations = (direction?: string) => {
    this.setState(
      { showOperations: !this.state.showOperations, loadingOps: true, direction, errorOps: null },
      () => {
        if (this.state.showOperations && direction) {
          const timeParams = { start: this.state.start, end: this.state.end };
          const cb = (operations) => {
            this.setState({ operations, loadingOps: false, activeOpsIndex: 0 });
          };
          const errcb = (err: IOperationErrorResponse) => {
            const newState = {
              loadingOps: false,
              errorOps: null,
            };
            if (typeof err === 'string') {
              newState.errorOps = err;
            }
            if (typeof err === 'object' && err !== null) {
              if (typeof err.response === 'string') {
                newState.errorOps = err.response;
              } else {
                newState.errorOps = JSON.stringify(err);
              }
            }
            this.setState(newState);
          };
          getOperations(
            this.state.target,
            timeParams,
            this.state.activeField.name,
            direction,
            cb,
            errcb
          );
        }
      }
    );
  };

  private nextOperation = () => {
    this.setState({
      activeOpsIndex: this.state.activeOpsIndex + 1,
    });
  };

  private prevOperation = () => {
    this.setState({
      activeOpsIndex: this.state.activeOpsIndex - 1,
    });
  };

  private handleExpandFields = (namespace: string, tablename: string, type: string) => {
    const entityId = getTableId(tablename, namespace, type);
    const relatedFields =
      type === 'cause'
        ? this.state.causeSets[entityId].fields
        : this.state.impactSets[entityId].fields;

    const allSets = type === 'cause' ? this.state.causeSets : this.state.impactSets;
    const updatedSets = { ...allSets };

    // If we already the unrelated fields for this table, no need to fetch again - just update expansion state for that table
    if (allSets[entityId].hasOwnProperty('unrelatedFields')) {
      updatedSets[entityId].isExpanded = !updatedSets[entityId].isExpanded;
      this.setState(updatedSets);
      return;
    }

    fetchUnrelatedFields(
      namespace,
      tablename,
      type,
      relatedFields,
      this.state.start,
      this.state.end
    ).subscribe(
      (unrelatedFields) => {
        // Look up the sets to update (cause or impact)
        // Add unrelated fields to the appropriate entity in the cause or impact sets
        updatedSets[entityId].unrelatedFields = unrelatedFields;
        updatedSets[entityId].isExpanded = true;
        this.setState(updatedSets);
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.error('Error getting unrelated fields', err);
      }
    );
  };

  public state = {
    target: '',
    targetFields: {},
    links: getDefaultLinks(),
    causeSets: {},
    impactSets: {},
    activeField: { id: null, name: null },
    showingOneField: false,
    start: null,
    end: null,
    selection: TIME_OPTIONS[1],
    activeCauseSets: {},
    activeImpactSets: {},
    activeLinks: null,
    loadingOps: false,
    loadingLineage: true,
    direction: null,
    // for handling pagination
    numTables: 4,
    firstCause: 1,
    firstImpact: 1,
    firstField: 1,
    showOperations: false,
    activeOpsIndex: 0,
    error: null,
    handleFieldClick: this.handleFieldClick,
    handleViewCauseImpact: this.handleViewCauseImpact,
    handleReset: this.handleReset,
    handleExpandFields: this.handleExpandFields,
    setTimeRange: this.setTimeRange,
    setCustomTimeRange: this.setCustomTimeRange,
    toggleOperations: this.toggleOperations,
    nextOperation: this.nextOperation,
    prevOperation: this.prevOperation,
  };

  public initialize() {
    const dataset = objectQuery(this.props, 'match', 'params', 'datasetId');
    const queryParams = parseQueryString();
    const timeParams = getTimeRangeFromUrl();
    this.fetchFieldLineage(queryParams, timeParams, dataset);
  }

  public componentDidUpdate(prevProps) {
    const existingDataset = objectQuery(prevProps, 'match', 'params', 'datasetId');
    const newDataset = objectQuery(this.props, 'match', 'params', 'datasetId');
    if (existingDataset !== newDataset) {
      this.initialize();
    }
  }

  public componentDidMount() {
    this.initialize();
  }

  public render() {
    return <FllContext.Provider value={this.state}>{this.props.children}</FllContext.Provider>;
  }
}

export function Consumer({ children }: { children: (context: IContextState) => React.ReactChild }) {
  return <FllContext.Consumer>{children}</FllContext.Consumer>;
}
