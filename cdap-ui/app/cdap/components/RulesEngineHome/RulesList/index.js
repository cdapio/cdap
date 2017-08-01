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

import React, {PropTypes} from 'react';
import {Row, Col} from 'reactstrap';
import {DragTypes} from 'components/RulesEngineHome/Rule';
import { DropTarget } from 'react-dnd';
import classnames from 'classnames';

require('./RulesList.scss');

function collect(connect, monitor) {
  return {
    connectDropTarget: connect.dropTarget(),
    isOver: monitor.isOver(),
    canDrop: monitor.canDrop()
  };
}

function RulesList({rules, onRemove, connectDropTarget, isOver}) {
  console.log('isOver', isOver);
  return connectDropTarget(
    <div className={classnames("rules-container", {
      'drag-hover': isOver
    })}>
      <div className="title"> Rules ({rules.length}) </div>
      {
        (!Array.isArray(rules) || (Array.isArray(rules) && !rules.length)) ?
          null
        :
          rules.map((rule, i) => {
            return (
              <Row>
                <Col xs={1}>{i + 1}</Col>
                <Col xs={3}>{rule.id}</Col>
                <Col xs={5}>{rule.description}</Col>
                <Col xs={3}>
                  <a onClick={() => onRemove(rule.id)}> Remove </a>
                </Col>
              </Row>
            );
          })
      }
    </div>
  );
}
RulesList.propTypes = {
  rules: PropTypes.arrayOf(PropTypes.object),
  onRemove: PropTypes.func,
  isOver: PropTypes.bool.isRequired,
  canDrop: PropTypes.bool.isRequired,
  connectDropTarget: PropTypes.func.isRequired
};

export default DropTarget(DragTypes.RULE, {}, collect)(RulesList);
