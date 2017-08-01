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
import {Input, Button} from 'reactstrap';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import {createNewRuleBook} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import MyRulesEngine from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import {getRuleBooks} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import moment from 'moment';
import RulesList from 'components/RulesEngineHome/RulesList';
import LoadingSVG from 'components/LoadingSVG';

require('./RuleBookDetails.scss');


export default class RuleBookDetails extends Component {
  state = {
    activeRuleBook: null,
    create: {
      name: '',
      description: '',
      rules: []
    }
  };

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {rulebooks} = RulesEngineStore.getState();
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
          getRuleBooks();
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
  }

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

  createRulebook = () => {
    createNewRuleBook(this.state.create);
  }

  renderCreateRulebook = () => {
    return (
      <div className="rule-book-create">
        <div className="create-metadata-container">
          <Input
            value={this.state.create.name}
            onChange={this.onNameChangeHandler}
            placeholder="Add Name"
          />
          <div>
            <span> Owner : </span>
            <span> Admin </span>
          </div>
          <div>
            <span> Created </span>
            <span> Created Today </span>
          </div>
          <textarea
            rows="10"
            className="form-control"
            value={this.state.create.description}
            onChange={this.onDescriptionChangeHandler}
            placeholder="Add Description"
          >
          </textarea>
          <div className="button-container">
            <Button
              color="primary"
              onClick={this.createRulebook}
              disabled={isEmpty(this.state.create.name)}
            >
              Create
            </Button>
          </div>
        </div>
         <RulesList rules={[]} />
      </div>
    );
  };

  renderEmptyView = () => {
    return (
      <div className="rule-book-details empty">
        <h2> No Rulebooks added </h2>
        <div>
          Please
          <a onClick={() => this.setState({ createMode: true })}> click here </a>
          to add one
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
      return (<LoadingSVG />);
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
            <span> Owner: </span>
            <span> {rulebookDetails.user}</span>
          </div>
          <div>
            <span>Last Updated on: </span>
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
              onRemove={this.removeRule}
            />
        }
      </div>
    );
  }
}

