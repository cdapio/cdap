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
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
require('./CaskMarketPlace.less');

import Tabs from '../Tabs';
import TabContents from '../TabContents';
import TabContent from '../TabContent';
import TabHeaders from '../TabHeaders';
import TabHead from '../TabHead';
export default class CaskMarketPlace extends Component{
  constructor(props) {
    super(props);
    this.state = {
      tabId: 0,
      tabs:[
        {
          title: 'All',
          content: 'All Tab content'
        },
        {
          title: 'Examples',
          content: 'Examples Tab Content'
        },
        {
          title: 'Use Cases',
          content: 'Use cases Tab Content'
        },
        {
          title: 'Pipeline',
          content: 'Pipeline Tab Content'
        },
        {
          title: 'Applications',
          content: 'Applications Tab Content'
        },
        {
          title: 'Datasets',
          content: 'Datasets Tab Content'
        },
        {
          title: 'Plugins',
          content: 'Plugins Tab Content'
        },
        {
          title: 'Dashboards',
          content: 'Dashboards Tab Content'
        },
        {
          title: 'Artifacts',
          content: 'Artifacts Tab Content'
        }
      ]
    };
  }
  componentWillReceiveProps() {
    // On closing CaskMarketPlace modal reset the tab to be 0 instead of the one that is last opened.
    this.setState({
      tabId: 0
    });
  }
  setTab(tabId) {
    this.setState({tabId});
  }
  isActiveTab(tabId) {
    return this.state.tabId === tabId;
  }
  render() {
    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.onCloseHandler}
        className="cask-market-place"
        size="lg"
      >
        <ModalHeader className="cask-market-place-header">
          <span className="pull-left">
            CASK Market Place
          </span>
          <div className="pull-right">
            <button className="btn btn-sm btn-resource-center">
              Resource Center
            </button>
          </div>
        </ModalHeader>
        <ModalBody>
          <Tabs mode="vertical">
            <TabHeaders>
              {this.state.tabs.map((tab, index) => {
                return(
                  <TabHead
                    key={index}
                    onClick={() => this.setTab(index)}
                    activeTab={this.isActiveTab(index)}
                  >
                    <strong>{tab.title}</strong>
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
        </ModalBody>
      </Modal>
    );
  }
}

CaskMarketPlace.propTypes = {
  onCloseHandler: PropTypes.func,
  isOpen: PropTypes.bool
};
