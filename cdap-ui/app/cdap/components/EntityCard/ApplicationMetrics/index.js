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
import {MyAppApi} from 'api/app';
import NamespaceStore from 'services/NamespaceStore';
import {humanReadableNumber} from 'services/helpers';
import T from 'i18n-react';

export default class ApplicationMetrics extends Component {
  constructor(props) {
    super(props);

    this.state = {
      numPrograms: 0,
      running: 0,
      failed: 0,
      loading: true
    };
     this.updateState = this.updateState.bind(this);
  }
  componentWillReceiveProps(nextProps) {
    if (nextProps.entity.id !== this.props.entity.id) {
      this.fetchApplicationMetrics();
    }
  }
  updateState() {
    if (!this._mounted) {
      return;
    }
    this.setState.apply(this, arguments);
  }
  // Need a major refactor
  componentWillMount() {
    this._mounted = true;
    if (!this.props.entity.id) {
      return;
    } else {
      this.fetchApplicationMetrics();
    }
  }
  componentWillUnmount() {
    this._mounted = false;
  }
  fetchApplicationMetrics() {
    const params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      appId: this.props.entity.id
    };
    MyAppApi.get(params)
      .subscribe( (res) => {
        this.updateState({numPrograms: res.programs.length});

        const statusRequestArray = res.programs.map((program) => {
          return {
            appId: this.props.entity.id,
            programType: program.type.toLowerCase(),
            programId: program.id
          };
        });

        MyAppApi.batchStatus({
          namespace: NamespaceStore.getState().selectedNamespace
        }, statusRequestArray)
          .subscribe((stats) => {
            this.updateState({
              running: stats.filter((stat) => stat.status === 'RUNNING').length,
              failed: stats.filter((stat) => stat.status === 'FAILED').length,
              loading: false
            });
          });
      }, (err) => {
        console.log('ERROR', err);
      });
  }
  render () {
    const loading = <span className="fa fa-spin fa-spinner"></span>;

    return (
      <div className="metrics-container">
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.metrics.programs')}</p>
          <p>{this.state.loading ? loading : humanReadableNumber(this.state.numPrograms)}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.metrics.running')}</p>
          <p>{this.state.loading ? loading : humanReadableNumber(this.state.running)}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.metrics.failed')}</p>
          <p>{this.state.loading ? loading : humanReadableNumber(this.state.failed)}</p>
        </div>
      </div>
    );
  }
}

ApplicationMetrics.propTypes = {
  entity: PropTypes.object
};
