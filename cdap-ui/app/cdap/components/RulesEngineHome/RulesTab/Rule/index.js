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
import {Col, Form, FormGroup, Label} from 'reactstrap';
import IconSVG from 'components/IconSVG';
import moment from 'moment';
import classnames from 'classnames';
import MyRulesEngineApi from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import LoadingSVG from 'components/LoadingSVG';
import DSVEditor from 'components/DSVEditor';
import RulesEngineStore, {RULESENGINEACTIONS} from 'components/RulesEngineHome/RulesEngineStore';
import shortid from 'shortid';
import {preventPropagation} from 'services/helpers';
import RulebooksPopover from 'components/RulesEngineHome/RulesTab/Rule/RulebooksPopover';
import {getRuleBooks, setError} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import { DragSource } from 'react-dnd';
import T from 'i18n-react';

require('./Rule.scss');

const DragTypes = {
  RULE: 'RULE'
};

export {DragTypes};

const ruleSource = {
  beginDrag(props) {
    // Return the data describing the dragged item
    const rule = { rule: props.rule };
    return rule;
  },

  endDrag(props, monitor) {
    if (!monitor.didDrop()) {
      return;
    }
  }
};

function collect(connect, monitor) {
  return {
    connectDragSource: connect.dragSource(),
    isDragging: monitor.isDragging()
  };
}
class Rule extends Component {
  static propTypes = {
    rule: PropTypes.object,
    connectDragSource: PropTypes.func.isRequired,
    isDragging: PropTypes.bool.isRequired
  };

  state = {
    viewDetails: false,
    detailsLoading: false,
    ruleDetails: null,
    edit: false
  };

  componentDidMount() {
    this.rulesStoreSubscription = RulesEngineStore.subscribe(() => {
      let {rules} = RulesEngineStore.getState();
      if (rules.activeRuleId === this.props.rule.id) {
        this.fetchRuleDetails();
      } else {
        if (this.state.viewDetails) {
          this.setState({
            viewDetails: false
          });
        }
      }
    });
  }

  componentWillUnmount() {
    if (this.rulesStoreSubscription) {
      this.rulesStoreSubscription();
    }
  }

  viewDetails = () => {
    if (this.state.viewDetails) {
      RulesEngineStore.dispatch({
        type: RULESENGINEACTIONS.RESETACTIVERULE
      });
      return;
    }
    RulesEngineStore.dispatch({
      type: RULESENGINEACTIONS.SETACTIVERULE,
      payload: {
        activeRuleId: this.props.rule.id
      }
    });
  };

  fetchRuleDetails = () => {
    if (this.state.viewDetails) {
      return;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    this.setState({
      viewDetails: true,
      detailsLoading: true
    });
    MyRulesEngineApi
      .getRuleDetails({
        namespace,
        ruleid: this.props.rule.id
      })
      .subscribe(
        res => {
          this.setState({
            ruleDetails: res.values,
            detailsLoading: false
          });
        },
        () => {
          this.setState({
            detailsLoading: false
          });
        }
      );
  };

  onRulesChange(rows) {
    // TODO: add edit feature for rule in rules tab.
    console.log(rows);
  }

  onRuleBookSelect = (rulebookid) => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngineApi
      .addRuleToRuleBook({
        namespace,
        rulebookid,
        ruleid: this.props.rule.id
      })
      .subscribe(
        () => {
          getRuleBooks();
        },
        setError
      );
  }

  renderDetails = () => {
    if (this.state.detailsLoading) {
      return (
        <Col xs="12" className="text-xs-center">
          <LoadingSVG />
        </Col>
      );
    }
    let rules = this.state.ruleDetails.action.map(action => ({
      uniqueId: shortid.generate(),
      property: action
    }));
    return (
      <Col xs="12">
        <Form onSubmit={preventPropagation}>
          <FormGroup row>
            <Label sm={2}> {T.translate(`commons.when`)} </Label>
            <Col sm={10}>
              <textarea
                value={this.state.ruleDetails.condition}
                className="form-control"
                row={10}
                disabled>
              </textarea>
            </Col>
          </FormGroup>
          <FormGroup row>
            <Label sm={2}> {T.translate(`commons.then`)} </Label>
            <Col sm={10}>
              <fieldset disabled={!this.state.edit}>
                <DSVEditor
                  values={rules}
                  onChange={this.onRulesChange}
                  disabled={!this.state.edit}
                />
              </fieldset>
            </Col>
          </FormGroup>
          <div>
            <RulebooksPopover
              onChange={this.onRuleBookSelect}
            />
          </div>
        </Form>
      </Col>
    );
  }

  renderRow = () => {
    const { connectDragSource } = this.props;
    return (
      connectDragSource(
        <div onClick={this.viewDetails}>
          <Col xs="6">
            <div className="svg-arrow-wrapper">
              {
                !this.state.viewDetails ?
                  <IconSVG name="icon-caret-right" />
                :
                  <IconSVG name="icon-caret-down" />
              }
            </div>
            {this.props.rule.id}
          </Col>
          <Col xs="5">
            {moment(this.props.rule.updated * 1000).format('MM-DD-YYYY')}
          </Col>
          <Col xs="1">
            <IconSVG name="icon-bars" />
          </Col>
        </div>
      )
    );
  }

  render() {
    return (
        <div
          className={classnames("engine-rule row", {
            'expanded': this.state.viewDetails
          })}
        >
          {this.renderRow()}
          {
            this.state.viewDetails ?
              this.renderDetails()
            :
              null
          }
        </div>
    );
  }
}

export default DragSource(DragTypes.RULE, ruleSource, collect)(Rule);
