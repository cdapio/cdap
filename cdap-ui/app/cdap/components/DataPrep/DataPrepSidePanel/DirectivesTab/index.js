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
import uuidV4 from 'uuid/v4';
import DirectivesTabRow from 'components/DataPrep/DataPrepSidePanel/DirectivesTab/DirectivesTabRow';
import fileDownload from 'js-file-download';
import { execute } from 'components/DataPrep/store/DataPrepActionCreator';
import T from 'i18n-react';

require('./DirectivesTab.scss');
export default class DirectivesTab extends Component {
  constructor(props) {
    super(props);

    let store = DataPrepStore.getState().dataprep;

    this.state = {
      deleteHover: null,
      directives: store.directives.map((directive) => {
        let obj = {
          name: directive,
          uniqueId: uuidV4(),
        };
        return obj;
      }),
    };

    this.download = this.download.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;

      this.setState({
        directives: state.directives.map((directive) => {
          let obj = {
            name: directive,
            uniqueId: uuidV4(),
          };
          return obj;
        }),
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  onMouseEnter(index) {
    this.setState({ deleteHover: index });
  }
  onMouseLeave() {
    this.setState({ deleteHover: null });
  }

  deleteDirective(index) {
    let state = DataPrepStore.getState().dataprep;
    let directives = state.directives;

    let newDirectives = directives.slice(0, index);

    execute(newDirectives, true).subscribe(
      () => {},
      (err) => {
        // Should not ever come to this.. this is only if backend
        // fails somehow
        console.log('Error deleting directives', err);
      }
    );
  }

  download() {
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId,
      directives = state.directives;

    let data = directives.join('\n'),
      filename = `${workspaceId}-directives.txt`;

    fileDownload(data, filename);
  }

  render() {
    return (
      <div className="directives-tab">
        <div className="directives-tab-header">
          <span>#</span>
          <span>{T.translate('features.DataPrep.DataPrepSidePanel.DirectivesTab.label')}</span>
          <button className="btn btn-link float-right" onClick={this.download}>
            <span className="fa fa-download" />
          </button>
        </div>

        <div className="directives-tab-body">
          {this.state.directives.map((directive, index) => {
            return (
              <DirectivesTabRow
                key={directive.uniqueId}
                rowInfo={directive}
                rowIndex={index}
                isInactive={this.state.deleteHover !== null && index >= this.state.deleteHover}
                handleDelete={this.deleteDirective.bind(this, index)}
                handleMouseEnter={this.onMouseEnter.bind(this, index)}
                handleMouseLeave={this.onMouseLeave.bind(this)}
              />
            );
          })}
        </div>
      </div>
    );
  }
}
