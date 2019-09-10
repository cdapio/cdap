/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
require('./FeatureColumnModal.scss');

class FeatureColumnModal extends Component {
  gridApi;
  gridColumnApi;

  constructor(props) {
    super(props);
    this.state = {
      open: props.open,
      modelData: props.data,
    };
  }

  handleClose = () => {
    this.setState({ open: false });
  };

  toggle = () => {
    this.setState(prevState => ({ open: !prevState.open }));
  };

  componentWillReceiveProps(nextProps) {
    this.setState({
      open: nextProps.open,
      modelData: nextProps.data
    });
  }

  render() {

    return (
        <Modal
          isOpen={this.state.open}
          toggle={this.toggle.bind(this)}
          size="lg"
          zIndex="1061"
          className="model-detail-container cdap-modal">
          <ModalHeader>
            <span className="alert-dialog-title" >
              Model Details
            </span>
            <div className="close-section float-xs-right">
              <span
                className="fa fa-times"
                onClick={this.handleClose}
              />
            </div>
          </ModalHeader>
          <ModalBody>
            <div className="json-container">
              <pre>
                {JSON.stringify(this.state.modelData, null, 2)}
              </pre>
            </div>
          </ModalBody>
        </Modal>
        );
  }
}
export default FeatureColumnModal;
FeatureColumnModal.propTypes = {
  open: PropTypes.boolean,
  data: PropTypes.any,
};
