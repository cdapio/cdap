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
import {DragTypes} from 'components/RulesEngineHome/RulesTab/Rule';
import { DropTarget } from 'react-dnd';
import classnames from 'classnames';
import MyRulesEngineApi from 'api/rulesengine';
import NamespaceStore from 'services/NamespaceStore';
import {getRulesForActiveRuleBook, setError} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import update from 'react/lib/update';
import RulebookRule from 'components/RulesEngineHome/RuleBookDetails/RulebookRule';
import T from 'i18n-react';

require('./RulesList.scss');

const PREFIX = 'features.RulesEngine.RulesList';

const dropTarget = {
  drop: (props, monitor, component) => {
    let item = monitor.getItem();
    component.addRuleToRulebook(item.rule);
  }
};

function collect(connect, monitor) {
  return {
    connectDropTarget: connect.dropTarget(),
    isOver: monitor.isOver(),
    canDrop: monitor.canDrop()
  };
}


class RulesList extends Component {
  static propTypes = {
    rulebookid: PropTypes.string,
    rules: PropTypes.arrayOf(PropTypes.object),
    onRemove: PropTypes.func,
    connectDropTarget: PropTypes.func.isRequired,
    isOver: PropTypes.bool.isRequired,
    onRuleAdd: PropTypes.func,
    onRuleBookUpdate: PropTypes.func
  };

  state = {
    rulebookRules: this.props.rules
  };

  componentWillReceiveProps(nextProps) {
    this.setState({
      rulebookRules: nextProps.rules
    });
  }

  addRuleToRulebook(rule) {
    if (this.props.onRuleAdd) {
      this.props.onRuleAdd(rule);
      return;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngineApi
      .addRuleToRuleBook({
        namespace,
        rulebookid: this.props.rulebookid,
        ruleid: rule.id
      })
      .subscribe(
        () => {
          getRulesForActiveRuleBook();
        },
        setError
      );
  }

  onRulesSort = (dragIndex, hoverIndex) => {
    const { rulebookRules } = this.state;
    const dragRule = rulebookRules[dragIndex];

    this.setState(update(this.state, {
      rulebookRules: {
        $splice: [
          [dragIndex, 1],
          [hoverIndex, 0, dragRule],
        ],
      },
    }), () => {
      if (this.props.onRuleBookUpdate) {
        this.props.onRuleBookUpdate(this.state.rulebookRules);
      }
    });
  };

  render() {
    let rules = this.state.rulebookRules;
    return this.props.connectDropTarget(
      <div className={classnames("rules-container", {
        'drag-hover': this.props.isOver
      })}>
        <div className="title"> {T.translate(`${PREFIX}.rulesLabel`)} ({Array.isArray(rules) ? rules.length : 0}) </div>
        <div className="rules">
          {
            (!Array.isArray(rules) || (Array.isArray(rules) && !rules.length)) ?
              null
            :
              rules.map((rule, i) => {
                return (
                  <RulebookRule
                    key={rule.id}
                    index={i}
                    rule={rule}
                    onRuleSort={this.onRulesSort}
                    onRemove={this.props.onRemove}
                  />
                );
              })
          }
          <div className="drag-drop-placeholder">
            {T.translate(`${PREFIX}.dropContainerText`)}
          </div>
        </div>
      </div>
    );
  }
}

export default DropTarget(DragTypes.RULE, dropTarget, collect)(RulesList);
