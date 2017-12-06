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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import isNil from 'lodash/isNil';
import MyRulesEngine from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import {getRulesForActiveRuleBook, resetCreateRuleBook, setError} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import moment from 'moment';
import RulesList from 'components/RulesEngineHome/RuleBookDetails/RulesList';
import LoadingSVG from 'components/LoadingSVG';
import CreateRulebook from 'components/RulesEngineHome/CreateRulebook';
import MyRulesEngineApi from 'api/rulesengine';
import RulebookMenu from 'components/RulesEngineHome/RuleBookDetails/RulebookMenu';
import T from 'i18n-react';
import { MyMetricApi } from 'api/metric';
import {RadialChart} from 'react-vis';
import cloneDeep from 'lodash/cloneDeep';

require('./RuleBookDetails.scss');

const PREFIX = 'features.RulesEngine.RulebookDetails';

export default class RuleBookDetails extends Component {

  static propTypes = {
    onApply: PropTypes.func
  };

  state = {
    activeRuleBook: null,
    rulebookDetails: null,
    createMode: false,
    loading: true,
    onApplying: false
  };

  updateState = () => {
    let {rulebooks} = RulesEngineStore.getState();
    if (isNil(rulebooks.list)) {
      return;
    }
    let rb = cloneDeep(rulebooks);
    let activeRulebook = rb.activeRulebookId;
    let createMode = rb.createRulebook;
    let rulebookDetails = rb.list.find(rb => rb.id === activeRulebook) || {};
    rulebookDetails.rules = rb.activeRulebookRules;
    // FIXME: This will fetch metrics whenever there is an update in the store.
    this.fetchRuleMetrics(rulebookDetails);
    this.setState({
      rulebookDetails,
      activeRuleBook: activeRulebook,
      createMode,
      loading: false
    });
  };

  fetchRuleMetrics(rulebookDetails = {rules: []}) {
    if (!Array.isArray(rulebookDetails.rules) || (Array.isArray(rulebookDetails.rules) && !rulebookDetails.rules.length)) {
      return;
    }
    let metrics = rulebookDetails.rules.map((rule) => {
      return `user.${rule.id}.fired`;
    });
    let {selectedNamespace:namespace} = NamespaceStore.getState();
    let postBody = {};
    postBody[`rule.metrics`] = {
      tags: {
        namespace: namespace,
        app: '*'
      },
      metrics : metrics,
      "timeRange": {
        end: "now",
        start : "now-1d"
      }
    };
    MyMetricApi
      .query(null, postBody)
      .subscribe(
        (response) => {
          let timeseries = response[`rule.metrics`].series;
          let metricValues = {};
          timeseries.map((series) => {
            metricValues[series.metricName] = series.data.map((d) => d.value);
          });
          let rulebookDetails = {...this.state.rulebookDetails};
          rulebookDetails.rules = rulebookDetails.rules.map((rule) => {
            return Object.assign({}, rule, {metric : metricValues[`user.${rule.id}.fired`]});
          });
          this.setState({rulebookDetails});
        }
      );
  }

  componentDidMount() {
    this.updateState();
    this.rulesStoreSubscription = RulesEngineStore.subscribe(this.updateState);
  }

  componentWillUnmount() {
    if (this.rulesStoreSubscription) {
      this.rulesStoreSubscription();
    }
  }

  onApply = () => {
    this.setState({
      onApplying: true
    });
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let rulebookid = this.state.rulebookDetails.id;
    MyRulesEngine
      .getRulebook({
        namespace,
        rulebookid
      })
      .subscribe(
        (res) => {
          let rulebook = res.values[0];
          this.props.onApply(rulebook, rulebookid);
        }
      );
  };

  updateRulebook = (rules) => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let urlparams = {
      namespace,
      rulebookid: this.state.rulebookDetails.id
    };
    let headers = {'Content-Type': 'application/json'};
    let postBody = {
      ...this.state.rulebookDetails,
      rules: rules.map(rule => rule.id)
    };
    MyRulesEngineApi
      .updateRulebook(urlparams, postBody, headers)
      .subscribe(
        () => {
          getRulesForActiveRuleBook();
        },
        setError
      );
  };

  removeRule = (ruleid) => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngine
      .removeRuleFromRuleBook({
        namespace,
        rulebookid: this.state.rulebookDetails.id,
        ruleid
      })
      .subscribe(
        () => {
          getRulesForActiveRuleBook();
        },
        setError
      );
  };

  generateAggregateStats = () => {
    let metricTotal = {};
    let total = 0;
    if (Array.isArray(this.state.rulebookDetails.rules) && this.state.rulebookDetails.rules.length) {
      this.state.rulebookDetails.rules.map((rule) => {
        if (isNil(rule.metric)) {
          return;
        }
        rule.metric.map((point) => {
          if (!metricTotal[rule.id]) {
            metricTotal[rule.id] = point;
          } else {
            metricTotal[rule.id]+= point;
          }
          total += point;
        });
      });
      let angles = [];
      Object
        .keys(metricTotal)
        .forEach(metric => {
          angles.push({angle : metricTotal[metric] / total});
        });
      return angles;
    }
    return null;
  };

  renderCreateRulebook = () => {
    return (
      <CreateRulebook
        onCancel={() => {
          resetCreateRuleBook();
        }}
      />
    );
  };

  renderEmptyView = () => {
    return (
      <div className="rule-book-details empty">
        <h2> {T.translate(`${PREFIX}.norulebooks`)} </h2>
        <div>
          {T.translate('commons.please')}
          <a onClick={() => this.setState({ createMode: true })}> {T.translate(`commons.clickhere`)} </a>
          {T.translate(`${PREFIX}.addone`)}
        </div>
      </div>
    );
  };

  render() {
    let {integration} = RulesEngineStore.getState();
    if (this.state.createMode) {
      return this.renderCreateRulebook();
    }

    if (this.state.loading) {
      return (
        <div className="rule-book-details loading">
          <LoadingSVG />
        </div>
      );
    }

    if (isNil(this.state.activeRuleBook)) {
      return this.renderEmptyView();
    }
    let {rulebookDetails} = this.state;
    let angles = this.generateAggregateStats();
    return (
      <div className="rule-book-details">
        <div className="rule-book-name-header">
          <div className="rule-book-name-version">
            <strong
              className="rule-book-name"
              title={rulebookDetails.id}
            >
              {rulebookDetails.id}
            </strong>
            <p className="rule-book-version">
              {T.translate(`${PREFIX}.version`, {version: rulebookDetails.version})}
            </p>
          </div>
          <div className="rulebook-menu-container">
            {
              integration.embedded ?
                <button
                  className="btn btn-primary"
                  onClick={this.onApply}
                  disabled={this.state.onApplying}
                >
                  {
                    this.state.onApplying ? <LoadingSVG /> : null
                  }
                  <span>{T.translate(`${PREFIX}.applyBtnLabel`)}</span>
                </button>
              :
                null
            }
            <RulebookMenu
              rulebookid={rulebookDetails.id}
              embedded={integration.embedded}
            />
          </div>
        </div>
        <div className="rule-book-content">
          <div className="rule-book-metadata">
            <div>
              <div>
                <strong> {T.translate(`${PREFIX}.owner`)}: </strong>
                <span> {rulebookDetails.user}</span>
              </div>
              <div>
                <strong>{T.translate(`${PREFIX}.lastmodified`)}: </strong>
                <span>{moment(rulebookDetails.updated * 1000).format('MM-DD-YY HH:mm')}</span>
              </div>
              <p className="rule-book-description">
                {rulebookDetails.description}
              </p>
            </div>
            <div>
              {
                isNil(angles) ? null : <RadialChart data={angles} width={100} height={100}/>
              }
            </div>
          </div>
          <RulesList
            rules={rulebookDetails.rules}
            rulebookid={rulebookDetails.id}
            onRemove={this.removeRule}
            onRuleBookUpdate={this.updateRulebook}
          />
        </div>
      </div>
    );
  }
}

