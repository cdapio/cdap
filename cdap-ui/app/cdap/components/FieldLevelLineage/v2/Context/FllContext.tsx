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
import { objectQuery, parseQueryString } from 'services/helpers';
import { getCurrentNamespace } from 'services/NamespaceStore';
import {
  IField,
  ILink,
  ITableFields,
  ITimeParams,
  getTableId,
  getTimeRange,
  getTimeRangeFromUrl,
  replaceHistory,
  getFieldLineage,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import * as d3 from 'd3';
import { TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';

const defaultContext: IContextState = {
  target: '',
  targetFields: [],
  links: [],
  causeSets: {},
  impactSets: {},
  showingOneField: false,
  start: null,
  end: null,
  selection: TIME_OPTIONS[1],
};

export const FllContext = React.createContext<IContextState>(defaultContext);

type ITimeType = number | string | null;

export interface IContextState {
  target: string;
  targetFields: IField[];
  links: ILink[];
  causeSets: ITableFields;
  impactSets: ITableFields;
  showingOneField: boolean;
  start: ITimeType;
  end: ITimeType;
  selection: string;
  handleFieldClick?: (event: React.MouseEvent<HTMLDivElement>) => void;
  handleViewCauseImpact?: () => void;
  handleReset?: () => void;
  activeField?: IField;
  activeCauseSets?: ITableFields;
  activeImpactSets?: ITableFields;
  activeLinks?: ILink[];
  numTables?: number;
  firstCause?: number;
  firstImpact?: number;
  firstField?: number;
  setTimeRange?: (range: string) => void;
  setCustomTimeRange?: ({ start, end }) => void;
}

export class Provider extends React.Component<{ children }, IContextState> {
  private handleFieldClick = (e: React.MouseEvent<HTMLDivElement>) => {
    const activeFieldId = (e.target as HTMLDivElement).id; // casting because EventTarget lacks target and name
    if (!activeFieldId) {
      return;
    }
    if (this.state.activeField) {
      d3.select(`#${this.state.activeField.id}`).classed('selected', false);
    }

    const newField = {
      id: activeFieldId,
      name: (e.target as HTMLDivElement).dataset.fieldname,
    };
    this.setState(
      {
        activeField: newField,
        activeLinks: this.getActiveLinks(activeFieldId),
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

  private getActiveLinks = (activeFieldId: string = this.state.activeField.id) => {
    const activeLinks = [];
    this.state.links.forEach((link) => {
      const isSelected = link.source.id === activeFieldId || link.destination.id === activeFieldId;
      if (isSelected) {
        activeLinks.push(link);
      }
    });
    return activeLinks;
  };

  private getActiveSets = () => {
    const activeCauseSets = {};
    const activeImpactSets = {};
    let activeLinks = this.state.activeLinks;

    if (!this.state.activeLinks) {
      activeLinks = this.getActiveLinks();
    }

    activeLinks.forEach((link) => {
      // for each link, look at id prefix to find the field that is not the target and add to the activeCauseSets or activeImpactSets
      const nonTargetFd = link.source.type !== 'target' ? link.source : link.destination;
      const tableId = getTableId(nonTargetFd.dataset, nonTargetFd.namespace, nonTargetFd.type);

      if (nonTargetFd.type === 'cause') {
        if (!(tableId in activeCauseSets)) {
          activeCauseSets[tableId] = [];
        }
        activeCauseSets[tableId].push(nonTargetFd);
      } else {
        if (!(tableId in activeImpactSets)) {
          activeImpactSets[tableId] = [];
        }
        activeImpactSets[tableId].push(nonTargetFd);
      }
    });

    this.setState(
      {
        activeLinks,
        activeCauseSets,
        activeImpactSets,
      },
      () => {
        this.setState({
          showingOneField: true,
        });
      }
    );
  };

  private handleViewCauseImpact = () => {
    this.getActiveSets();
  };

  private handleReset = () => {
    this.setState({
      showingOneField: false,
    });
  };

  private fetchFieldLineage(qParams, timeParams, dataset = this.state.target) {
    const namespace = getCurrentNamespace();
    const updateState = (newState) => this.setState(newState);
    getFieldLineage(namespace, dataset, qParams, timeParams, updateState);
  }

  private updateLineageFromRange(selection: string, start: ITimeType, end: ITimeType) {
    const newState = {
      selection,
      start: null,
      end: null,
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

  public state = {
    target: '',
    targetFields: [],
    links: [],
    causeSets: {},
    impactSets: {},
    activeField: null,
    showingOneField: false,
    start: null,
    end: null,
    selection: TIME_OPTIONS[1],
    activeCauseSets: null,
    activeImpactSets: null,
    activeLinks: null,
    // for handling pagination
    numTables: 4,
    firstCause: 1,
    firstImpact: 1,
    firstField: 1,
    handleFieldClick: this.handleFieldClick,
    handleViewCauseImpact: this.handleViewCauseImpact,
    handleReset: this.handleReset,
    setTimeRange: this.setTimeRange,
    setCustomTimeRange: this.setCustomTimeRange,
  };

  public initialize() {
    const dataset = objectQuery(this.props, 'match', 'params', 'datasetId');
    const queryParams = parseQueryString();
    const timeParams = getTimeRangeFromUrl();
    this.fetchFieldLineage(queryParams, timeParams, dataset);
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
