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
      tabId: '1'
    };
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
            <button className="btn btn-resource-center">
              Resource Center
            </button>
          </div>
        </ModalHeader>
        <ModalBody>
          <Tabs mode="vertical">
            <TabHeaders>
              <TabHead
                onClick={() => this.setTab('1')}
                activeTab={this.isActiveTab('1')}
              >
                Tab 1
              </TabHead>
              <TabHead
                onClick={() => this.setTab('2')}
                activeTab={this.isActiveTab('2')}
              >
                Tab 2
              </TabHead>
            </TabHeaders>
            <TabContents activeTab={this.state.tabId}>
              <TabContent name="1">
                Tab 1 Contents
              </TabContent>
              <TabContent name="2">
                Tab 2 Contents
              </TabContent>
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
