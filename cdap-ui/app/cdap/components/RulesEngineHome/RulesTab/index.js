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
import {Col, Row, Button, Input, Container} from 'reactstrap';
import IconSVG from 'components/IconSVG';
import moment from 'moment';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';

require('./RulesTab.scss');

export default class RulesTab extends Component {
  state = {
    rules: [],
    searchStr: ''
  };

  updateSearchStr = (e) => {
    this.setState({
      searchStr: e.target.value
    });
  };

  addRule =() => {
    console.log('TODO: Create a new rule');
  };

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {rules} = RulesEngineStore.getState();
      this.setState({
        rules
      });
    });
  }

  render() {
    if (!this.state.rules.length) {
      return null;
    }
    return (
      <div className="rules-tab">
        <Input
          placeholder="Search Rulebook by name, owner or description"
          value={this.state.searchStr}
          onChange={this.updateSearchStr}
        />
        <Button onClick={this.addRule}>
          Create a New Rule
        </Button>
        <Container>
          <Row>
            <Col xs="8">
              <div className="name">
                Name
              </div>
            </Col>
            <Col xs="4">
              Date
            </Col>
          </Row>
          {
            this
              .state
              .rules
              .map(rule => {
                return (
                  <Row>
                    <Col xs="8">
                      <div className="svg-arrow-wrapper">
                        <IconSVG name="icon-caret-right" />
                      </div>
                      {rule.id}
                    </Col>
                    <Col xs="4">
                      {moment(rule.updated * 1000).format('MM-DD-YYYY')}
                    </Col>
                  </Row>
                );
              })
            }
        </Container>
      </div>
    );
  }
}
