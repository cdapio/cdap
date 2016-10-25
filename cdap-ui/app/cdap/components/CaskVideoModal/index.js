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
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import CaskVideo from '../CaskVideo';
require('./CaskVideoModal.less');

export default class CaskVideoModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      viewMode: 'video'
    };
  }

  closeHandler() {
    this.props.onCloseHandler();
  }

  render() {

    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.closeHandler.bind(this)}
        className="cask-video-modal"
      >
        <ModalHeader>
          <span className="pull-left">
          </span>
        </ModalHeader>
        <ModalBody>
          <ReactCSSTransitionGroup
            transitionName="cask-video-modal-content"
            transitionEnterTimeout={500}
            transitionLeaveTimeout={300}
          >
            {
              <div className="video-container">
                <CaskVideo />
              </div>
            }
          </ReactCSSTransitionGroup>
        </ModalBody>
      </Modal>
    );
  }
}

CaskVideoModal.propTypes = {
  onCloseHandler: PropTypes.func,
  isOpen: PropTypes.bool
};
