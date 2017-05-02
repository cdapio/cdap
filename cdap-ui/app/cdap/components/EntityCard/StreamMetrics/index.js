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
import {MyStreamApi} from 'api/stream';
import NamespaceStore from 'services/NamespaceStore';
import {humanReadableNumber, HUMANREADABLESTORAGE_NODECIMAL} from 'services/helpers';
import T from 'i18n-react';

export default class StreamMetrics extends Component {
  constructor(props) {
    super(props);

    this.state = {
      programs: 0,
      events: 0,
      bytes: 0,
      loading: true
    };
  }

  componentWillMount() {
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    const streamParams = {
      namespace: currentNamespace,
      streamId: this.props.entity.id
    };
    const metricsParams = {
      tag: [`namespace:${currentNamespace}`, `stream:${this.props.entity.id}`],
      metric: ['system.collect.events', 'system.collect.bytes'],
      aggregate: true
    };

    MyMetricApi.query(metricsParams)
      .combineLatest(MyStreamApi.getPrograms(streamParams))
      .subscribe((res) => {
        let events = 0,
            bytes = 0;
        if (res[0].series.length > 0) {
          res[0].series.forEach((metric) => {
            if (metric.metricName === 'system.collect.events') {
              events = humanReadableNumber(metric.data[0].value);
            } else if (metric.metricName === 'system.collect.bytes') {
              bytes = humanReadableNumber(metric.data[0].value, HUMANREADABLESTORAGE_NODECIMAL);
            }
          });
        }

        this.setState({
          events,
          bytes,
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
          <p className="metric-header">{T.translate('commons.entity.stream.programs')}</p>
          <p>{this.state.loading ? loading : this.state.programs}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.stream.events')}</p>
          <p>{this.state.loading ? loading : this.state.events}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.stream.bytes')}</p>
          <p>{this.state.loading ? loading : this.state.bytes}</p>
        </div>
      </div>
    );
  }
}

StreamMetrics.propTypes = {
  entity: PropTypes.object
};
