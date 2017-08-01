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
import {Row, Col, Input, Form, FormGroup, Label} from 'reactstrap';
import {preventPropagation} from 'services/helpers';
import DSVEditor from 'components/DSVEditor';
import MyRulesEngine from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import {getRules} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import RulesEngineStore, {RULESENGINEACTIONS} from 'components/RulesEngineHome/RulesEngineStore';
import isEmpty from 'lodash/isEmpty';

require('./CreateRule.scss');

export default class CreateRule extends Component {
  static propTypes = {
    onClose: PropTypes.func
  };

  componentDidMount() {
    if (this.nameRef) {
      setTimeout(() => this.nameRef.focus(), 1);
    }
  }

  state = {
    when: null,
    description: '',
    then: [{
      property: '',
      uniqueId: 0
    }],
    name: ''
  };

  onNameChange = (e) => {
    this.setState({
      name: e.target.value
    });
  };
  onRulesChange = (conditions) => {
    this.setState({
      then: conditions
    });
  };
  onConditionChange = (e) => {
    this.setState({
      when: e.target.value
    });
  };

  onDescriptionChange = (e) => {
    this.setState({
      description: e.target.value
    });
  };

  isActionsEmpty = () => {
    return isEmpty(this.state.then.map(action => action.property).join(''));
  };

  isApplyBtnDisabled = () => {
    return isEmpty(this.state.name) || isEmpty(this.state.description) || isEmpty(this.state.when) || this.isActionsEmpty();
  }

  createRule = () => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let config = {};
    let {name: id, when, then, description} = this.state;
    then = then.map(clause => clause.property);
    config = {id, description, when, then};
    MyRulesEngine
      .createRule({ namespace }, config)
      .subscribe(
        () => {
          getRules();
          this.props.onClose();
        },
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
  }

  render() {
    return (
      <div className="create-rule-container">
        <Row>
          <Col xs="7">
            <Input
              value={this.state.name}
              onChange={this.onNameChange}
              placeholder="Add Name for Rule"
              getRef={(ref) => this.nameRef = ref}
            />
          </Col>
          <Col xs="5">
            Today
          </Col>
          <Col xs="12">
            <Form onSubmit={preventPropagation} className="when-then-clause-container">
              <FormGroup row>
                <Label sm={4}> Description </Label>
                <Col sm={8}>
                  <textarea
                    value={this.state.description}
                    onChange={this.onDescriptionChange}
                    className="form-control"
                    row={10}
                  >
                  </textarea>
                </Col>
              </FormGroup>
              <FormGroup row>
                <Label sm={2}> When </Label>
                <Col sm={10}>
                  <textarea
                    value={this.state.when}
                    onChange={this.onConditionChange}
                    className="form-control"
                    row={15}>
                  </textarea>
                </Col>
              </FormGroup>
              <FormGroup row>
                <Label sm={2}> Then </Label>
                <Col sm={10}>
                  <DSVEditor
                    values={this.state.then}
                    onChange={this.onRulesChange}
                    placeholder="Action"
                  />
                </Col>
              </FormGroup>
            </Form>
          </Col>
        </Row>
        <hr />
        <div className="btn-container">
          <button
            className="btn btn-primary"
            onClick={this.createRule}
            disabled={this.isApplyBtnDisabled()}
          >
            Apply
          </button>
          <div
            className="btn btn-secondary"
            onClick={this.props.onClose}
          >
            Cancel
          </div>
        </div>
      </div>
    );
  }
}
