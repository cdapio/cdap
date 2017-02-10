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

import Market from '../Market';
import ResourceCenter from '../ResourceCenter';

import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import classNames from 'classnames';
import T from 'i18n-react';

require('./PlusButtonModal.scss');

export default class PlusButtonModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      viewMode: 'marketplace'
    };

    this.closeHandler = this.closeHandler.bind(this);
  }
  toggleView(tab) {
    if (this.state.viewMode === tab) {
      return;
    }
    this.setState({
      viewMode: tab
    });
  }
  closeHandler() {
    this.setState({
      viewMode: 'marketplace'
    });
    this.props.onCloseHandler();
  }

  getIconForView(view, opposite) {
    const resourceCenterClass = 'icon-resourcecenter';
    const marketIconClass = 'icon-CaskMarket';

    if (opposite) {
      return view === 'resourcecenter' ? marketIconClass : resourceCenterClass;
    } else {
      return view === 'resourcecenter' ? resourceCenterClass : marketIconClass;
    }
  }
  render() {
    const market = T.translate('commons.market');
    const resourceCenter = T.translate('commons.resource-center');

    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.closeHandler.bind(this)}
        className="plus-button-modal"
        size="lg"
      >
        <ModalHeader>
          <span className="float-xs-left">
            <span
              className={classNames("modal-header-icon", this.getIconForView(this.state.viewMode))}
            />
            <span className="plus-modal-header-text">
            { this.state.viewMode === 'resourcecenter' ? resourceCenter : market }
            </span>
          </span>
          <div className="float-xs-right">
            <div
              className="modal-close-btn"
              onClick={this.closeHandler}
            >
              <span className="fa fa-times" />
            </div>
          </div>
        </ModalHeader>
        <ModalBody>
          <ReactCSSTransitionGroup
            transitionName="plus-button-modal-content"
            transitionEnterTimeout={500}
            transitionLeaveTimeout={300}
          >
            {
              this.state.viewMode === 'marketplace' ? <Market key="1"/> : <ResourceCenter key="2"/>
            }
          </ReactCSSTransitionGroup>
        </ModalBody>
      </Modal>
    );
  }
}

PlusButtonModal.defaultProps = {
  mode: 'marketplace'
};

PlusButtonModal.propTypes = {
  onCloseHandler: PropTypes.func,
  isOpen: PropTypes.bool,
  mode: PropTypes.oneOf(['marketplace', 'resourcecenter'])
};
