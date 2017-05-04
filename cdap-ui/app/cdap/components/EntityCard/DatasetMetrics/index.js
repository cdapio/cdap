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
import {MyMetricApi} from 'api/metric';
import {MyDatasetApi} from 'api/dataset';
import NamespaceStore from 'services/NamespaceStore';
import {humanReadableNumber} from 'services/helpers';
import T from 'i18n-react';

export default class DatasetMetrics extends Component {
  constructor(props) {
    super(props);

    this.state = {
      programs: 0,
      ops: 0,
      writes: 0,
      loading: true
    };
  }

  componentWillMount() {
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    const datasetParams = {
      namespace: currentNamespace,
      datasetId: this.props.entity.id
    };
    const metricsParams = {
      tag: [`namespace:${currentNamespace}`, `dataset:${this.props.entity.id}`],
      metric: ['system.dataset.store.ops', 'system.dataset.store.writes'],
      aggregate: true
    };

    MyMetricApi.query(metricsParams)
      .combineLatest(MyDatasetApi.getPrograms(datasetParams))
      .subscribe((res) => {
        let ops = 0,
            writes = 0;
        if (res[0].series.length > 0) {
          res[0].series.forEach((metric) => {
            if (metric.metricName === 'system.dataset.store.ops') {
              ops = metric.data[0].value;
            } else if (metric.metricName === 'system.dataset.store.writes') {
              writes = metric.data[0].value;
            }
          });
        }

        this.setState({
          ops,
          writes,
          programs: res[1].length,
          loading: false
        });
      });
  }

  render () {
    const loading = <span className="fa fa-spin fa-spinner"></span>;

    return (
      <div className="metrics-container">
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.dataset.programs')}</p>
          <p>{this.state.loading ? loading : humanReadableNumber(this.state.programs)}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.dataset.operations')}</p>
          <p>{this.state.loading ? loading : humanReadableNumber(this.state.ops)}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.dataset.writes')}</p>
          <p>{this.state.loading ? loading : humanReadableNumber(this.state.writes)}</p>
        </div>
      </div>
    );
  }
}

DatasetMetrics.propTypes = {
  entity: PropTypes.object
};
