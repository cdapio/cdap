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
import { Nav, NavItem, TabPane, TabContent, NavLink} from 'reactstrap';
import classnames from 'classnames';
import RuleBooksTab from 'components/RulesEngineHome/RuleBooksTab';
import RulesTab from 'components/RulesEngineHome/RulesTab';
import {getRuleBooks, getRules} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import RuleBookDetails from 'components/RulesEngineHome/RuleBookDetails';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import {Provider} from 'react-redux';
import {RuleBookCountWrapper, RulesCountWrapper} from 'components/RulesEngineHome/RulesEngineTabCounters';
import RulesEngineAlert from 'components/RulesEngineHome/RulesEngineAlert';
import getDndContextProvider from 'components/RulesEngineHome/DnDContextProvider';

require('./RulesEngineHome.scss');
var DnDContextProvider = getDndContextProvider();

class RulesEngineHome extends Component {
  state = {
    activeTab: '1'
  };

  componentDidMount() {
    getRuleBooks();
    getRules();
  }

  toggleTab = (activeTab) => {
    this.setState({
      activeTab
    });
  }

  render() {
    return (
      <div className="rules-engine-home">
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
                    Rules Books (
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
                    Rules (
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
          <RuleBookDetails />
        </div>
        <RulesEngineAlert />
      </div>
    );
  }
}
export default DnDContextProvider(RulesEngineHome);
