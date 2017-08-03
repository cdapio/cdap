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
import RuleBookDetails from 'components/RulesEngineHome/RuleBookDetails';
import RuleBooksTab from 'components/RulesEngineHome/RuleBooksTab';
import RulesTab from 'components/RulesEngineHome/RulesTab';
import {getRuleBooks, getRules} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import {Provider} from 'react-redux';
import {RuleBookCountWrapper, RulesCountWrapper} from 'components/RulesEngineHome/RulesEngineTabCounters';
import RulesEngineAlert from 'components/RulesEngineHome/RulesEngineAlert';
import { DragDropContext } from 'react-dnd';
import HTML5Backend from 'react-dnd-html5-backend';
import NamespaceStore from 'services/NamespaceStore';
import MyRulesEngineApi from 'api/rulesengine';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import RulesEngineServiceControl from 'components/RulesEngineHome/RulesEngineServiceControl';
import Helmet from 'react-helmet';
import T from 'i18n-react';

require('./RulesEngineHome.scss');
const PREFIX = 'features.RulesEngine.Home';

class RulesEngineHome extends Component {
  state = {
    activeTab: '1',
    loading: true,
    backendDown: false
  };

  componentDidMount() {
    this.checkIfBackendUp();
  }

  fetchRulesAndRulebooks = () => {
    getRuleBooks();
    getRules();
  }

  toggleTab = (activeTab) => {
    this.setState({
      activeTab
    });
  }

  checkIfBackendUp() {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngineApi
      .ping({namespace})
      .subscribe(
        () => {
          this.setState({
            loading: false
          });
          this.fetchRulesAndRulebooks();
        },
        () => {
          this.setState({
            backendDown: true,
            loading: false
          });
        }
      );
  }

  onServiceStart = () => {
    this.setState({
      loading: false,
      backendDown: false
    });
    this.fetchRulesAndRulebooks();
  };

  render() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }

    if (this.state.backendDown) {
      return (
        <RulesEngineServiceControl
          onServiceStart={this.onServiceStart}
        />
      );
    }

    return (
      <div className="rules-engine-home">
        <Helmet
          title={T.translate(`${PREFIX}.pageTitle`)}
        />
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
          <RuleBookDetails />
        </div>
        <RulesEngineAlert />
      </div>
    );
  }
}
export default DragDropContext(HTML5Backend)(RulesEngineHome);
