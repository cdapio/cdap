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
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import isNil from 'lodash/isNil';
require('./RuleBookDetails.scss');

export default class RuleBookDetails extends Component {
  state = {
    activeRuleBook: null
  };

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {rulebooks} = RulesEngineStore.getState();
      let activeRulebook = rulebooks.activeRulebookId;
      let rulebookDetails = rulebooks.list.find(rb => rb.id === activeRulebook) || {};
      this.setState({
        rulebookDetails,
        activeRuleBook: activeRulebook
      });
    });
  }

  renderRules(rules) {
    if (!Array.isArray(rules) || (Array.isArray(rules) && !rules.length)) {
      return null;
    }
    return (
      rules.map((rule, i) => {
        return (
          <tr>
            <td>{i+1} </td>
            <td>{rule.id}</td>
            <td>{rule.description}</td>
            <td>Remove</td>
          </tr>
        );
      })
    );
  }

  render() {
    if (isNil(this.state.activeRuleBook)) {
      return null;
    }
    let {rulebookDetails} = this.state;
    let {rulebooks} = RulesEngineStore.getState();
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
            <span>Created on </span>
            <span>{rulebookDetails.created}</span>
          </div>
        </div>
        <p>
          {rulebookDetails.description}
        </p>

        <div className="rules-container">
          <div className="title"> Rules ({rules.length}) </div>
          <table className="table">
            <tbody>
              {this.renderRules(rules)}
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}
