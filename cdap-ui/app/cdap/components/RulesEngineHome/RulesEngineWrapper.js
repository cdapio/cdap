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
import { Nav, NavItem, TabPane, TabContent, NavLink} from 'reactstrap';
import classnames from 'classnames';
import RuleBookDetails from 'components/RulesEngineHome/RuleBookDetails';
import RuleBooksTab from 'components/RulesEngineHome/RuleBooksTab';
import RulesTab from 'components/RulesEngineHome/RulesTab';
import {Provider} from 'react-redux';
import {RuleBookCountWrapper, RulesCountWrapper} from 'components/RulesEngineHome/RulesEngineTabCounters';
import T from 'i18n-react';
import { DragDropContext } from 'react-dnd';
import HTML5Backend from 'react-dnd-html5-backend';
import RulesEngineStore, {RULESENGINEACTIONS} from 'components/RulesEngineHome/RulesEngineStore';

require('./RulesEngineWrapper.scss');
const PREFIX = 'features.RulesEngine.Home';

class RulesEngineWrapper extends Component {

  static propTypes = {
    onApply: PropTypes.func
  };

  state = {
    activeTab: '1',
  };

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {rulebooks} = RulesEngineStore.getState();
      if (rulebooks.activeTab && rulebooks.activeTab !== this.state.activeTab) {
        this.setState({
          activeTab: rulebooks.activeTab
        });
      }
    });
  }

  toggleTab = (activeTab) => {
    RulesEngineStore.dispatch({
      type: RULESENGINEACTIONS.SETACTIVETAB,
      payload: {
        activeTab
      }
    });
  };

  render () {
    return (
      <div className="rules-engine-wrapper">
        <div className="left-panel">
          <Nav tabs>
            <NavItem>
              <div onClick={this.toggleTab.bind(this, '1')}>
                <NavLink
                  className={classnames({
                    'active': this.state.activeTab == '1'
                  })}
                >
                  <strong>
                    {T.translate(`${PREFIX}.Tabs.rbTitle`)} (
                      <Provider store={RulesEngineStore}>
                        <RuleBookCountWrapper />
                      </Provider>
                    )
                  </strong>
                </NavLink>
              </div>
            </NavItem>
            <NavItem>
              <div  onClick={this.toggleTab.bind(this, '2')}>
                <NavLink
                  className={classnames({
                    'active': this.state.activeTab == '2'
                  })}
                >
                  <strong>
                    {T.translate(`${PREFIX}.Tabs.rulesTitle`)} (
                      <Provider store={RulesEngineStore}>
                        <RulesCountWrapper />
                      </Provider>
                    )
                  </strong>
                </NavLink>
              </div>
            </NavItem>
            </Nav>
            <TabContent activeTab={this.state.activeTab}>
              <TabPane tabId="1">
                {
                  this.state.activeTab === '1' ?
                    <RuleBooksTab />
                  :
                    null
                }
              </TabPane>
              <TabPane tabId="2">
                {
                  this.state.activeTab === '2' ?
                    <RulesTab />
                  :
                    null
                }
              </TabPane>
            </TabContent>
        </div>
        <div className="right-panel">
          <RuleBookDetails onApply={this.props.onApply}/>
        </div>
      </div>
    );
  }
}

export default DragDropContext(HTML5Backend)(RulesEngineWrapper);
