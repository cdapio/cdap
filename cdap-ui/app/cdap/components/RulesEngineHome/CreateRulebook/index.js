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
import {Input, Button} from 'reactstrap';
import isEmpty from 'lodash/isEmpty';
import RulesList from 'components/RulesEngineHome/RuleBookDetails/RulesList';
import {createNewRuleBook} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import T from 'i18n-react';

const PREFIX = 'features.RulesEngine.CreateRulebook';

export default class CreateRulebook extends Component {
  state = {
    name: '',
    description: '',
    rules: []
  };

  onNameChangeHandler = (e) => {
    this.setState({
      name: e.target.value
    });
  };

  onDescriptionChangeHandler = (e) => {
    this.setState({
      description: e.target.value
    });
  };

  onRulesAdd = (rule) => {
    let isRuleAlreadyExist = this.state.rules.find(r => rule.id === r.id);
    if (isRuleAlreadyExist) {
      return;
    }
    this.setState({
      rules: [...this.state.rules, rule]
    });
  };

  onRemove = (ruleid) => {
    this.setState({
      rules: this.state.rules.filter(rule => rule.id !== ruleid)
    });
  }

  createRulebook = () => {
    let config = {};
    let {name, description, rules} = this.state;
    rules = rules.map(rule => rule.id);
    config = {id: name, description, rules};
    createNewRuleBook(config);
  };

  render() {
    return (
      <div className="rule-book-create">
        <div className="create-metadata-container">
          <Input
            value={this.state.name}
            onChange={this.onNameChangeHandler}
            placeholder={T.translate(`${PREFIX}.nameplaceholder`)}
          />
          <div>
            <span> {T.translate(`${PREFIX}.owner`)} : </span>
            <span> {T.translate(`${PREFIX}.admin`)} </span>
          </div>
          <div>
            <span> {T.translate(`${PREFIX}.created`)} </span>
            <span> {T.translate(`${PREFIX}.createdToday`)} </span>
          </div>
          <textarea
            rows="10"
            className="form-control"
            value={this.state.description}
            onChange={this.onDescriptionChangeHandler}
            placeholder={T.translate(`${PREFIX}.descriptionplaceholder`)}
          >
          </textarea>
          <div className="button-container">
            <Button
              color="primary"
              onClick={this.createRulebook}
              disabled={isEmpty(this.state.name)}
            >
              {T.translate(`${PREFIX}.createBtnLabel`)}
            </Button>
          </div>
        </div>
         <RulesList
          rules={this.state.rules}
          onRuleAdd={this.onRulesAdd}
          onRemove={this.onRemove}
        />
      </div>
    );
  }
}

CreateRulebook.propTypes = {
  onCancel: PropTypes.func.isRequired
};
