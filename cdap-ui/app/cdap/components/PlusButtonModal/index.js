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
import CaskMarketPlace from '../CaskMarketPlace';
import ResourceCenter from '../ResourceCenter';

import ReactCSSTransitionGroup from 'react-addons-css-transition-group';

require('./PlusButtonModal.less');

export default class PlusButtonModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      viewMode: 'marketplace'
    };
  }
  toggleView() {
    this.setState({
      viewMode: this.state.viewMode === 'marketplace' ? 'resourcecenter' : 'marketplace'
    });
  }
  closeHandler() {
    this.setState({
      viewMode: 'marketplace'
    });
    this.props.onCloseHandler();
  }
  render() {
    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.closeHandler.bind(this)}
        className="plus-button-modal"
        size="lg"
      >
        <ModalHeader>
          <span className="pull-left">
            { this.state.viewMode === 'resourcecenter' ? 'Resource Center' : 'Cask Market Place' }
          </span>
          <div className="pull-right">
            <button
              className="btn btn-sm btn-resource-center"
              onClick={this.toggleView.bind(this)}
            >
              { this.state.viewMode === 'resourcecenter' ? 'Cask Market Place' : 'Resource Center' }
            </button>
          </div>
        </ModalHeader>
        <ModalBody>
          <ReactCSSTransitionGroup
            transitionName="plus-button-modal-content"
            transitionEnterTimeout={500}
            transitionLeaveTimeout={300}
          >
            {
              this.state.viewMode === 'marketplace' ? <CaskMarketPlace key="1"/> : <ResourceCenter key="2"/>
            }
          </ReactCSSTransitionGroup>
        </ModalBody>
      </Modal>
    );
  }
}

PlusButtonModal.propTypes = {
  onCloseHandler: PropTypes.func,
  isOpen: PropTypes.bool
};
