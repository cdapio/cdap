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
import {Col, Row, Button, Input} from 'reactstrap';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import Rule from 'components/RulesEngineHome/RulesTab/Rule';
import Fuse from 'fuse.js';
import isEmpty from 'lodash/isEmpty';
import CreateRule from 'components/RulesEngineHome/CreateRule';
import isNil from 'lodash/isNil';
import LoadingSVG from 'components/LoadingSVG';


require('./RulesTab.scss');

export default class RulesTab extends Component {
  state = {
    rules: RulesEngineStore.getState().rules.list,
    searchStr: '',
    createRule: false
  };

  updateSearchStr = (e) => {
    this.setState({
      searchStr: e.target.value
    });
  };

  addRule =() => {
    this.setState({
      createRule: true
    });
  };

  resetCreateRule = () => {
    this.setState({
      createRule: false
    });
  };

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {rules} = RulesEngineStore.getState();
      this.setState({
        rules: rules.list
      });
    });
  }

  getFilteredRules() {
    if (isEmpty(this.state.searchStr)) {
      return this.state.rules;
    }

    // TODO not sure about performance
    const fuseOptions = {
      caseSensitive: true,
      threshold: 0,
      location: 0,
      distance: 100,
      maxPatternLength: 32,
      keys: [
        "id",
        "description",
        "action"
      ]
    };

    let fuse = new Fuse(this.state.rules, fuseOptions);
    return fuse.search(this.state.searchStr);
  }

  renderRules(rules) {
    if (isNil(rules)) {
      return (<LoadingSVG />);
    }

    if (!rules.length && !this.state.createRule) {
      return (<h4 className="text-xs-center"> No Rules found </h4>);
    }

    return (
      <div className="container">
        <Row>
          <Col xs="7">
            Name
          </Col>
          <Col xs="5">
            Last Updated
          </Col>
        </Row>
        {
          this.state.createRule ?
            <CreateRule onClose={this.resetCreateRule}/>
          :
            null
        }
        {
          rules.map(rule => {
            return (
              <Rule rule={rule} />
            );
          })
        }
      </div>
    );
  }

  render() {
    let rules = this.getFilteredRules();

    return (
      <div className="rules-tab">
        <Input
          placeholder="Search Rules by name, action or description"
          value={this.state.searchStr}
          onChange={this.updateSearchStr}
        />
        <Button onClick={this.addRule}>
          Create a New Rule
        </Button>
        {this.renderRules(rules)}
      </div>
    );
  }
}
