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
import {createNewRuleBook} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import T from 'i18n-react';

const PREFIX = 'features.RulesEngine.CreateRulebook';

export default class CreateRulebook extends Component {
  static propTypes = {
    onCancel: PropTypes.func.isRequired
  };

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
          <div className="rule-book-name-header">
            <Input
              value={this.state.name}
              className="rule-book-name"
              onChange={this.onNameChangeHandler}
              placeholder={T.translate(`${PREFIX}.nameplaceholder`)}
            />
            <p className="rule-book-version">
              {T.translate(`${PREFIX}.version`, {version: 1})}
            </p>
          </div>
          <div className="rule-book-metadata">
            <div>
              <strong> {T.translate(`${PREFIX}.owner`)} : </strong>
              <span> {T.translate(`${PREFIX}.admin`)} </span>
            </div>
            <div>
              <strong> {T.translate(`${PREFIX}.created`)} : </strong>
              <span> {T.translate(`${PREFIX}.now`)} </span>
            </div>
          </div>
          <textarea
            rows="5"
            className="form-control rule-book-description"
            value={this.state.description}
            onChange={this.onDescriptionChangeHandler}
            placeholder={T.translate(`${PREFIX}.descriptionplaceholder`)}
          >
          </textarea>
          <p className="fields-required-text">
            <i>{T.translate('features.RulesEngine.shared.allFieldsRequired')}</i>
          </p>
          <div className="button-container">
            <Button
              color="secondary"
              onClick={this.createRulebook}
              disabled={isEmpty(this.state.name) || isEmpty(this.state.description)}
            >
              {T.translate(`${PREFIX}.createBtnLabel`)}
            </Button>
            <span className="create-next">
              {T.translate(`${PREFIX}.createBtnNext`)}
            </span>
          </div>
        </div>
      </div>
    );
  }
}

