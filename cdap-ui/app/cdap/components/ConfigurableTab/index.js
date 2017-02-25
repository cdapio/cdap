/*
 * Copyright © 2016 Cask Data, Inc.
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
import React, {PropTypes, Component} from 'react';

import Tabs from '../Tabs';
import TabHeaders from '../TabHeaders';
import {TabContent, TabPane} from 'reactstrap';

import TabHead from '../TabHead';
require('./ConfigurableTab.scss');

export default class ConfigurableTab extends Component {
  constructor(props) {
    super(props);
    /* Eventually we will be adding more to the state,
      - Steps completed
      - Steps with errors
      - Short description (not sure)
      - Long description (not sure)
      - classNames for individual sections
        - left tab section?
        - right container section?
        - highlighting tabs (both completed and errored/missing tabs)
      - and more...
    */
    let {tabs, layout, defaultTab} = this.props.tabConfig;
    this.state = { tabId: defaultTab, tabs, layout, defaultTab };
  }
  setTab(tabId) {
    this.setState({tabId});
    document.querySelector('.tab-content').scrollTop = 0;

    if (typeof this.props.onTabClick === 'function') {
      this.props.onTabClick(tabId);
    }
  }
  isActiveTab(tabId) {
    return this.state.tabId === tabId;
  }
  componentWillReceiveProps(nextProps) {
    this.setState({ tabs: nextProps.tabConfig.tabs});
  }
  render() {
    return (
      <div className="cask-configurable-tab">
        <Tabs layout={this.state.layout}>
          <TabHeaders>
            {this.state.tabs.map((tab, index) => {
              return (
                <TabHead
                  layout={this.state.layout}
                  key={index}
                  onClick={() => this.setTab(tab.id)}
                  activeTab={this.isActiveTab(tab.id)}
                >
                  <span className={`${tab.icon} tab-header-icon`}></span>
                  <span title={tab.name}>{tab.name}</span>
                </TabHead>
              );
            })}
          </TabHeaders>
          <TabContent activeTab={this.state.tabId}>
            {
              this.state.tabs
                .filter((tab) => tab.id === this.state.tabId)
                .map((tab, index) => {
                  return (
                    <TabPane
                      tabId={tab.id}
                      key={index}
                    >
                      {tab.content}
                    </TabPane>
                  );
                }
              )
            }
          </TabContent>
        </Tabs>
      </div>
    );
  }
}
const TabConfig = PropTypes.shape({
  name: PropTypes.string,
  content: PropTypes.node
});
ConfigurableTab.propTypes = {
  onTabClick: PropTypes.func,
  tabConfig: PropTypes.shape({
    tabs: PropTypes.arrayOf(TabConfig),
    layout: PropTypes.string,
    defaultTab: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.number
    ])
  })
};
