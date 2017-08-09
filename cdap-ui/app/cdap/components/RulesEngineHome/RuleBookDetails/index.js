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

import React, {Component} from 'react';
import RulesEngineStore, {RULESENGINEACTIONS}  from 'components/RulesEngineHome/RulesEngineStore';
import isNil from 'lodash/isNil';
import MyRulesEngine from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import {getRulesForActiveRuleBook, resetCreateRuleBook} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import moment from 'moment';
import RulesList from 'components/RulesEngineHome/RuleBookDetails/RulesList';
import LoadingSVG from 'components/LoadingSVG';
import CreateRulebook from 'components/RulesEngineHome/CreateRulebook';
import debounce from 'lodash/debounce';
import MyRulesEngineApi from 'api/rulesengine';
import T from 'i18n-react';

require('./RuleBookDetails.scss');

const PREFIX = 'features.RulesEngine.RulebookDetails';

export default class RuleBookDetails extends Component {
  state = {
    activeRuleBook: null,
    createMode: false
  };

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {rulebooks} = RulesEngineStore.getState();
      if (!rulebooks.list) {
        return;
      }
      let activeRulebook = rulebooks.activeRulebookId;
      let createMode = rulebooks.createRulebook;
      let rulebookDetails = rulebooks.list.find(rb => rb.id === activeRulebook) || {};
      this.setState({
        rulebookDetails,
        activeRuleBook: activeRulebook,
        createMode
      });
    });
  }

  updateRulebook = debounce((rules) => {
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
        () => {},
        (err) => {
          RulesEngineStore.dispatch({
            type: RULESENGINEACTIONS.SETERROR,
            payload: {
              error: {
                showError: true,
                message: typeof err === 'string' ? err : err.response.message
              }
            }
          });
        }
      );
  }, 2000);

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
        err => {
          RulesEngineStore.dispatch({
            type: RULESENGINEACTIONS.SETERROR,
            payload: {
              error: {
                showError: true,
                message: typeof err === 'string' ? err : err.response.message
              }
            }
          });
        }
      );
  };

  onNameChangeHandler = (e) => {
    this.setState({
      create: Object.assign({}, this.state.create, {
        name: e.target.value
      })
    });
  };

  onDescriptionChangeHandler = (e) => {
    this.setState({
      create: Object.assign({}, this.state.create, {
        description: e.target.value
      })
    });
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
  }

  render() {
    let {rulebooks} = RulesEngineStore.getState();
    if (this.state.createMode) {
      return this.renderCreateRulebook();
    }

    if (isNil(rulebooks.list)) {
      return (
        <div className="rule-book-details loading">
          <LoadingSVG />
        </div>
      );
    }

    if (isNil(this.state.activeRuleBook) && !rulebooks.list.length) {
      return this.renderEmptyView();
    }
    let {rulebookDetails} = this.state;

    let rules = rulebooks.activeRulebookRules;
    return (
      <div className="rule-book-details">
        <h3>{rulebookDetails.id}</h3>
        <div className="rule-book-metadata">
          <div>
            <span> {T.translate(`${PREFIX}.owner`)}: </span>
            <span> {rulebookDetails.user}</span>
          </div>
          <div>
            <span>{T.translate(`${PREFIX}.lastupdated`)}: </span>
            <span>{moment(rulebookDetails.updated * 1000).format('MM-DD-YY HH:mm')}</span>
          </div>
        </div>
        <p>
          {rulebookDetails.description}
        </p>
        {
          isNil(rules) ?
            <LoadingSVG />
          :
            <RulesList
              rules={rules}
              rulebookid={rulebookDetails.id}
              onRemove={this.removeRule}
              onRuleBookUpdate={this.updateRulebook}
            />
        }
      </div>
    );
  }
}

