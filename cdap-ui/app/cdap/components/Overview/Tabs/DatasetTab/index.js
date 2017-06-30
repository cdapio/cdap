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
import React, {Component, PropTypes} from 'react';
import T from 'i18n-react';
require('./DatasetTab.scss');
import {objectQuery} from 'services/helpers';
import ViewSwitch from 'components/ViewSwitch';
import DatasetStreamCards from 'components/DatasetStreamCards';
import DatasetStreamTable from 'components/DatasetStreamTable';
import NamespaceStore from 'services/NamespaceStore';
import {MyMetricApi} from 'api/metric';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import {humanReadableNumber, HUMANREADABLESTORAGE_NODECIMAL} from 'services/helpers';

export default class DatasetTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity,
      entitiesForTable: this.getEntitiesForTable(this.props.entity)
    };
    this.state.entity.datasets.forEach(this.addDatasetMetrics.bind(this));
    this.state.entity.streams.forEach(this.addStreamMetrics.bind(this));
  }
  componentWillReceiveProps(nextProps) {
    let entitiesMatch = objectQuery(nextProps, 'entity', 'name') === objectQuery(this.props, 'entity', 'name');
    if (!entitiesMatch) {
      this.setState({
        entity: nextProps.entity,
        entitiesForTable: this.getEntitiesForTable(nextProps.entity)
      });
      this.state.entity.datasets.forEach(this.addDatasetMetrics.bind(this));
      this.state.entity.streams.forEach(this.addStreamMetrics.bind(this));
    }
  }
  onTabSwitch() {
    this.state.entity.datasets.forEach(this.addDatasetMetrics.bind(this));
    this.state.entity.streams.forEach(this.addStreamMetrics.bind(this));
  }
  getEntitiesForTable({datasets, streams}) {
    return datasets
      .map(dataset => Object.assign({}, dataset, {type: 'datasetinstance', id: dataset.name}))
      .concat(
        streams
        .map(stream => Object.assign({}, stream, {type: 'stream', id: stream.name}))
      );
  }
  addStreamMetrics(stream) {
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    const streamParams = {
      namespace: currentNamespace,
      streamId: this.props.entity.name
    };
    const metricsParams = {
      tag: [`namespace:${currentNamespace}`, `stream:${stream.name}`],
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

        let entities = this.state.entitiesForTable
          .map(e => {
            if (e.name === stream.name) {
              return Object.assign({}, e, {
                events,
                reads: 'n/a',
                writes: 'n/a',
                bytes,
                programs: res[1].length,
                loading: false
              });
            }
            return e;
          });
        this.setState({
          entitiesForTable: entities
        });
      });
  }
  addDatasetMetrics(dataset) {
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    const datasetParams = {
      namespace: currentNamespace,
      datasetId: this.props.entity.name
    };
    const metricsParams = {
      tag: [`namespace:${currentNamespace}`, `dataset:${dataset.name}`],
      metric: ['system.dataset.store.bytes', 'system.dataset.store.writes', 'system.dataset.store.reads'],
      aggregate: true
    };

    MyMetricApi.query(metricsParams)
      .combineLatest(MyDatasetApi.getPrograms(datasetParams))
      .subscribe((res) => {
        let ops = 'n/a',
            writes = 0,
            bytes = 0,
            reads = 0;
        if (res[0].series.length > 0) {
          res[0].series.forEach((metric) => {
            if (metric.metricName === 'system.dataset.store.writes') {
              writes = metric.data[0].value;
            } else if (metric.metricName === 'system.dataset.store.bytes') {
              bytes = humanReadableNumber(metric.data[0].value, HUMANREADABLESTORAGE_NODECIMAL);
            } else if (metric.metricName === 'system.dataset.store.reads') {
              reads = metric.data[0].value;
            }
          });
        }

        let entities = this.state.entitiesForTable
          .map(e => {
            if (e.name === dataset.name) {
              return Object.assign({}, e, {
                events: ops,
                writes,
                reads,
                bytes,
                programs: res[1].length,
                loading: false
              });
            }
            return e;
          });
        this.setState({
          entitiesForTable: entities
        });
      });
  }
  render() {
    return (
      <div className="dataset-tab">
        <div className="message-section float-xs-left">
          <strong> {T.translate('features.Overview.DatasetTab.title', {appId: this.state.entity.name})} </strong>
        </div>
        {
          <ViewSwitch onSwitch={this.onTabSwitch.bind(this)}>
            <DatasetStreamCards dataEntities={this.state.entity.datasets.concat(this.state.entity.streams)} />
            <DatasetStreamTable dataEntities={this.state.entitiesForTable} />
          </ViewSwitch>
        }
      </div>
    );
  }
}

DatasetTab.propTypes = {
  entity: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    datasets: PropTypes.arrayOf(PropTypes.shape({
      id: PropTypes.string,
      type: PropTypes.string
    })),
    streams: PropTypes.arrayOf(PropTypes.shape({
      id: PropTypes.string,
      type: PropTypes.string
    }))
  }))
};
