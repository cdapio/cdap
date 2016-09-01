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

export default class CaskMarketPlace extends Component{
  constructor(props) {
    super(props);
  }
  render() {
    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.onCloseHandler}
        className="cask-market-place"
        size="lg"
      >
        <ModalHeader className="cask-market-place-header"> CASK Market Place </ModalHeader>
        <ModalBody>
          Some big Modal body
        </ModalBody>
      </Modal>
    );
  }
}

CaskMarketPlace.propTypes = {
  onCloseHandler: PropTypes.func,
  isOpen: PropTypes.bool
};
