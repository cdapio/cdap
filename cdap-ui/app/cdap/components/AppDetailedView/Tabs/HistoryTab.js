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

import React, {Component, PropTypes} from 'react';
import {MyProgramApi} from 'api/program';
import {convertProgramToApi} from 'services/program-api-converter';
import NamespaceStore from 'services/NamespaceStore';
import {humanReadableDate} from 'services/helpers';
import T from 'i18n-react';
import orderBy from 'lodash/orderBy';

require('./HistoryTab.less');

export default class HistoryTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      history: null
    };
    this.pollSubscriptions = [];
  }
  componentWillMount() {
    this.context
        .entity
        .programs
        .forEach(program => {
          let programType = convertProgramToApi(program.type);
          let programId = program.id;
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
              newRuns.map( r => {
                r.programName = programId;
                return r;
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
  render() {
    const renderHistoryRows = () => {
      if (Array.isArray(this.state.history)) {
        if (this.state.history.length) {
          return (
            <table className="app-detailed-view-history table table-bordered">
              <thead>
                <tr>
                  <th>Program Name </th>
                  <th>Start Time</th>
                  <th>Run ID</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {
                  this.state
                    .history
                    .map( history => {
                      return (
                        <tr key={history.runid}>
                          <td>{history.programName}</td>
                          <td>{humanReadableDate(history.start)}</td>
                          <td>{history.runid}</td>
                          <td>{history.status}</td>
                        </tr>
                      );
                    })
                }
              </tbody>
            </table>
          );
        } else {
          return (
            <h3 className="text-center empty-message">
              {T.translate('features.AppDetailedView.History.emptyMessage')}
            </h3>
          );
        }
      }
      return (
        <h3 className="text-center">
          <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
        </h3>
      );
    };
    return renderHistoryRows();
  }
}
HistoryTab.contextTypes = {
  entity: PropTypes.object
};
