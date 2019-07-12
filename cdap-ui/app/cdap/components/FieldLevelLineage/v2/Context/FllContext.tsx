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
  makeTargetFields,
  IField,
  ILink,
  ITableFields,
  getFieldsAndLinks,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import * as d3 from 'd3';

const defaultContext: IContextState = {
  target: '',
  targetFields: [],
  links: [],
  causeSets: {},
  impactSets: {},
  showingOneField: false,
};

export const FllContext = React.createContext<IContextState>(defaultContext);

export interface IContextState {
  target: string;
  targetFields: IField[];
  links: ILink[];
  causeSets: ITableFields;
  impactSets: ITableFields;
  showingOneField: boolean;
  handleFieldClick?: (event: React.MouseEvent<HTMLDivElement>) => void;
  handleViewCauseImpact?: () => void;
  handleReset?: () => void;
  activeField?: string;
  activeCauseSets?: ITableFields;
  activeImpactSets?: ITableFields;
  activeLinks?: ILink[];
  numTables?: number;
  firstCause?: number;
  firstImpact?: number;
  firstField?: number;
}

export class Provider extends React.Component<{ children }, IContextState> {
  private parsedRes = getFieldsAndLinks(data);

  private handleFieldClick = (e) => {
    const activeField = e.target.id;
    if (!activeField) {
      return;
    }
    d3.select(`#${this.state.activeField}`).classed('selected', false);

    this.setState(
      {
        activeField,
      },
      () => {
        d3.select(`#${activeField}`).classed('selected', true);
        this.getActiveLinks();
      }
    );
  };

  private getActiveLinks() {
    const activeFieldId = this.state.activeField;
    const activeLinks = [];
    this.state.links.forEach((link) => {
      const isSelected = link.source.id === activeFieldId || link.destination.id === activeFieldId;
      if (isSelected) {
        activeLinks.push(link);
      }
    });
    this.setState({ activeLinks });
  }

  private getActiveSets() {
    const activeCauseSets = {};
    const activeImpactSets = {};

    this.state.activeLinks.forEach((link) => {
      // for each link, look at id prefix to find the field that is not the target and add to the activeCauseSets or activeImpactSets
      const nonTargetFd = link.source.type !== 'target' ? link.source : link.destination;
      const tableToUpdate = nonTargetFd.type === 'cause' ? activeCauseSets : activeImpactSets;
      const tableId = `ns-${nonTargetFd.namespace}_ds-${nonTargetFd.dataset}`; // used as unique key to make sure we don't duplicate fields
      if (!(tableId in tableToUpdate)) {
        tableToUpdate[tableId] = [];
      }
      tableToUpdate[tableId].push(nonTargetFd);
    });

    this.setState(
      {
        activeCauseSets,
        activeImpactSets,
      },
      () => {
        this.setState({
          showingOneField: true,
        });
      }
    );
  }

  private handleViewCauseImpact = () => {
    this.getActiveSets();
  };

  private handleReset = () => {
    this.setState({
      showingOneField: false,
    });
  };

  public state = {
    target: data.entityId.dataset,
    targetFields: makeTargetFields(data.entityId, data.fields) as IField[],
    links: this.parsedRes.links,
    causeSets: this.parsedRes.causeTables,
    impactSets: this.parsedRes.impactTables,
    activeField: null,
    showingOneField: false,
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
  };

  public render() {
    return <FllContext.Provider value={this.state}>{this.props.children}</FllContext.Provider>;
  }
}

export function Consumer({ children }: { children: (context: IContextState) => React.ReactChild }) {
  return <FllContext.Consumer>{children}</FllContext.Consumer>;
}
