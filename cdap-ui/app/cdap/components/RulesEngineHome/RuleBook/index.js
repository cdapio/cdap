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
import moment from 'moment';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import classnames from 'classnames';
import {setActiveRulebook} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';

require('./RuleBook.scss');

export default function RuleBook({bookDetails}) {
  let {id, user:owner, created:createDate, rules, description} = bookDetails;
  let numOfRules = rules.split(',').length || 0;
  const onClick = () => {
    setActiveRulebook(id);
  };
  let {rulebooks} = RulesEngineStore.getState();

  return (
    <div
      onClick={onClick}
      className={classnames("rule-book", {
        active: rulebooks.activeRulebookId === id
      })}
    >
      <strong> {id} </strong>
      <div>
        <span> Owner: </span>
        <span> {owner} </span>
      </div>
      <div>
        <span>Created on </span>
        <span>{moment(createDate).format('MM-DD-YYYY')}</span>
      </div>
      <div>
        {numOfRules} Rules
      </div>
      <p>
        {description}
      </p>
    </div>
  );
}

RuleBook.defaultProps = {
  onClick: () => {}
};

RuleBook.propTypes = {
  bookDetails: PropTypes.object,
  onClick: PropTypes.func
};

