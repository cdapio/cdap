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
import NamespaceStore from 'services/NamespaceStore';
import {convertProgramToApi} from 'services/program-api-converter';
import T from 'i18n-react';

export default class ProgramMetrics extends Component {
  constructor(props) {
    super(props);

    this.state = {
      status: '',
      numRuns: 0,
      appName: this.props.entity.applicationId,
      loading: true
    };
    this.runsApi = '';
  }

  componentWillMount() {
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      appId: this.props.entity.applicationId,
      programType: convertProgramToApi(this.props.entity.programType),
      programId: this.props.entity.id
    };

    if (this.props.poll) {
      this.runsApi = MyProgramApi.pollRuns;
    } else {
      this.runsApi = MyProgramApi.runs;
    }
    this.programMetrics$ = this.runsApi(params)
      .combineLatest(MyProgramApi.pollStatus(params))
      .subscribe((res) => {
        this.setState({
          status: res[1].status,
          numRuns: res[0].length,
          loading: false
        });
      });
  }

  componentWillUnmount() {
    this.programMetrics$.dispose();
  }

  render () {
    const loading = <span className="fa fa-spin fa-spinner"></span>;

    return (
      <div className="metrics-container">
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.program.status')}</p>
          <p>{this.state.loading ? loading : this.state.status}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.program.runs')}</p>
          <p>{this.state.loading ? loading : this.state.numRuns}</p>
        </div>
        <div className="metric-item app-name">
          <p className="metric-header">{T.translate('commons.entity.program.application')}</p>
          <p>
            {
              this.state.loading ?
                loading
              :
                (
                  <span title={this.state.appName}>
                    {this.state.appName}
                  </span>
                )
            }
          </p>
        </div>
      </div>
    );
  }
}

ProgramMetrics.defaultProps = {
  poll: false
};

ProgramMetrics.propTypes = {
  entity: PropTypes.shape({
    applicationId: PropTypes.string.isRequired,
    programType: PropTypes.string.isRequired,
    id: PropTypes.string.isRequired
  }),
  poll: PropTypes.bool
};
