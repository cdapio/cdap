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
import TabContents from '../TabContents';
import TabContent from '../TabContent';
import TabHeaders from '../TabHeaders';
import TabHead from '../TabHead';
require('./Wizard.less');

export default class Wizard extends Component{
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
      - a Redux store with a common api for wizard?? O-o
      - and more...
    */
    let {tabs, mode, defaultTab} = this.props.wizardConfig;
    this.state = { tabId: defaultTab, tabs, mode, defaultTab };
  }
  setTab(tabId) {
    this.setState({tabId});
  }
  isActiveTab(tabId) {
    return this.state.tabId === tabId;
  }
  render() {
    return (
      <div className="cask-wizard">
        <Tabs mode={this.state.mode}>
          <TabHeaders>
            {this.state.tabs.map((tab, index) => {
              return(
                <TabHead
                  mode={this.state.mode}
                  key={index}
                  onClick={() => this.setTab(index)}
                  activeTab={this.isActiveTab(index)}
                >
                  <span>{tab.title}</span>
                </TabHead>
              );
            })}
          </TabHeaders>
          <TabContents activeTab={this.state.tabId}>
            {
              this.state.tabs.map((tab, index) => {
                return (
                  <TabContent
                    name={index}
                    key={index}
                  >
                    {tab.content}
                  </TabContent>
                );
              })
            }
          </TabContents>
        </Tabs>
      </div>
    );
  }
}
const TabConfig = PropTypes.shape({
  title: PropTypes.string,
  content: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.node
  ])
});
Wizard.propTypes = {
  wizardConfig: PropTypes.shape({
    tabs: PropTypes.arrayOf(TabConfig),
    mode: PropTypes.string,
    defaultTab: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.number
    ])
  })
};
