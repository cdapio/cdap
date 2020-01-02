/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, { Component } from 'react';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import CutDirective from 'components/DataPrep/Directives/ExtractFields/UsingPositions/CutDirective';
import MaskSelection from 'components/DataPrep/Directives/MaskData/MaskSelection';

import isNil from 'lodash/isNil';
require('./ColumnHighlighter.scss');

const directiveComponentMap = {
  CUT: CutDirective,
  MASK: MaskSelection,
};

export default class ColumnHighlighter extends Component {
  constructor(props) {
    super(props);
    let { highlightColumns } = DataPrepStore.getState().dataprep;
    this.state = {
      highlightColumns: highlightColumns,
    };
    this.hideColumnHighlight = this.hideColumnHighlight.bind(this);
  }
  componentDidMount() {
    this.datastoreSubscription = DataPrepStore.subscribe(() => {
      let highlightColumns = DataPrepStore.getState().dataprep.highlightColumns;
      let { directive } = highlightColumns;
      if (!isNil(directive)) {
        this.setState({
          highlightColumns,
        });
      }
      if (directive !== this.state.highlightColumns.directive) {
        this.setState({
          highlightColumns,
        });
      }
    });
  }
  componentWillUnmount() {
    if (this.datastoreSubscription) {
      this.datastoreSubscription();
    }
  }
  hideColumnHighlight() {
    DataPrepStore.dispatch({
      type: DataPrepActions.setHighlightColumns,
      payload: {
        highlightColumns: {
          columns: [],
          directive: null,
        },
      },
    });
  }
  render() {
    let { directive, columns } = this.state.highlightColumns;
    let Tag = directiveComponentMap[directive];

    if (isNil(Tag)) {
      return null;
    }

    return (
      <div className="dataprep-column-highlight">
        <Tag columns={columns} onClose={this.hideColumnHighlight} />
      </div>
    );
  }
}
