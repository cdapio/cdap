/*
 * Copyright © 2016 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {MyProgramApi} from 'api/program';
import {convertProgramToApi} from 'services/program-api-converter';
import NamespaceStore from 'services/NamespaceStore';
import {humanReadableDate} from 'services/helpers';
import LogAction from 'components/FastAction/LogAction';
import SortableTable from 'components/SortableTable';
import T from 'i18n-react';
import orderBy from 'lodash/orderBy';

require('./HistoryTab.scss');

export default class HistoryTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      history: null,
      entity: props.entity
    };
    this.pollSubscriptions = [];
    this.tableHeaders = [
      {
        property: 'programName',
        label: T.translate('features.AppDetailedView.History.nameLabel')
      },
      {
        property: 'start',
        label: T.translate('features.AppDetailedView.History.startLabel')
      },
      {
        label: T.translate('features.AppDetailedView.History.runIDLabel')
      },
      {
        property: 'status',
        label: T.translate('features.AppDetailedView.History.statusLabel')
      },
      {
        label: T.translate('features.FastAction.logLabel')
      }
    ];
  }
  componentWillMount() {
    this.state
        .entity
        .programs
        .forEach(program => {
          let programType = convertProgramToApi(program.type);
          let programId = program.name;
          let appId = program.app;
          let namespace = NamespaceStore.getState().selectedNamespace;
          this.pollSubscriptions.push(
            MyProgramApi
            .pollRuns({ namespace, programId, programType, appId})
            .subscribe(res => {
              let newRuns;
              if (this.state.history) {
                newRuns = res.filter(runRecord => {
                  return !this.state.history.filter( existingRun => existingRun.runid === runRecord.runid).length;
                });
              } else {
                newRuns = res;
              }
              let history = [...(this.state.history || [])];
              history = history.map(runRecord => {
                let runFromBackend = res.find(r => r.runid === runRecord.runid);
                if (!runFromBackend) {
                  return runRecord;
                }
                runRecord.status = runFromBackend.status;
                runRecord.end = runRecord.end !== runFromBackend.end ? runFromBackend.end : runRecord.end;
                return runRecord;
              });
              newRuns = newRuns.map( r => {
                return Object.assign({}, r, {
                  programName: programId,
                  programType,
                  appId
                });
              });
              res = orderBy([
                ...newRuns,
                ...history,
              ], ['start'], ['desc']);

              this.setState({
                history: res
              });
            })
          );
        });
  }

  componentWillUnmount() {
    this.pollSubscriptions
      .forEach(subscription => {
        subscription.dispose();
      });
  }

  renderTableBody(history) {
    let historyState = history || this.state.history;
    return (
      <tbody>
        {
          historyState
            .map( history => {
              return (
                <tr key={history.runid}>
                  <td>{history.programName}</td>
                  <td>{humanReadableDate(history.start)}</td>
                  <td>{history.runid}</td>
                  <td>{history.status}</td>
                  <td>
                    <div className="fast-actions-container">
                      <h4 className="text-xs-left btn-group">
                        <LogAction
                          entity={{
                            id: history.programName,
                            uniqueId: history.runid,
                            runId: history.runid,
                            applicationId: history.appId,
                            programType: history.programType
                          }}
                        />
                      </h4>
                    </div>
                  </td>
                </tr>
              );
            })
        }
      </tbody>
    );
  }

  render() {
    const renderHistoryRows = () => {
      if (Array.isArray(this.state.history)) {
        if (this.state.history.length) {
          return (
            <div className="history-tab">
              <SortableTable
                entities={this.state.history}
                tableHeaders={this.tableHeaders}
                renderTableBody={this.renderTableBody}
              />
            </div>
          );
        } else {
          return (
            <div className="history-tab">
              <i>
                {T.translate('features.AppDetailedView.History.emptyMessage')}
              </i>
            </div>
          );
        }
      }
      return (
        <div className="history-tab">
          <h3 className="text-xs-center">
            <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
          </h3>
        </div>
      );
    };
    return renderHistoryRows();
  }
}
HistoryTab.propTypes = {
  entity: PropTypes.object
};
