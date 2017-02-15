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
import React, {Component, PropTypes} from 'react';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
require('./HydratorPipeline.scss');
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import NamespaceStore from 'services/NamespaceStore';
import PlusButtonStore from 'services/PlusButtonStore';

import T from 'i18n-react';

export default class HydratorPipeline extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isOpen: this.props.isOpen,
      batchPipelineUrl: window.getHydratorUrl({
        stateName: 'hydrator.create',
        stateParams: {
          namespace: NamespaceStore.getState().selectedNamespace,
          artifactType: 'cdap-data-pipeline'
        }
      }),
      realtimePipelineUrl: window.getHydratorUrl({
        stateName: 'hydrator.create',
        stateParams: {
          namespace:  NamespaceStore.getState().selectedNamespace,
          artifactType: 'cdap-data-streams'
        }
      })
    };
  }
  componentWillReceiveProps(newProps) {
    this.setState({
      isOpen: newProps.isOpen
    });
  }
  closeHandler() {
    this.setState({
      isOpen: !this.state.isOpen
    });
    this.props.onClose();
  }
  closeModal() {
    this.closeHandler();
    PlusButtonStore.dispatch({
      type: 'TOGGLE_PLUSBUTTON_MODAL',
      payload: {
        modalState: false
      }
    });
  }
  render() {
    return (
      <Modal
        isOpen={this.state.isOpen}
        toggle={this.closeHandler.bind(this)}
        className="wizard-modal create-stream-wizard hydrator-pipeline-wizard-modal"
        size="lg"
        backdrop='static'
      >
        <ModalHeader>
          <span className="float-xs-left">
            {T.translate('features.Wizard.HydratorPipeline.title')}
          </span>
          <div className="close-section float-xs-right">
            <span
              className="fa fa-times"
              onClick={this.closeHandler.bind(this)}
            >
            </span>
          </div>
        </ModalHeader>
        <ModalBody>
          <ReactCSSTransitionGroup
            transitionName="plus-button-modal-content"
            transitionEnterTimeout={500}
            transitionLeaveTimeout={300}
          >
            <div className="hydrator-pipeline-content-container">
              <div className="message">
                {T.translate('features.Wizard.HydratorPipeline.message')}
              </div>
              <div className="action-buttons">
                <a
                  href={this.state.batchPipelineUrl}
                  onClick={this.closeModal.bind(this)}
                  className="btn btn-secondary"
                >
                  <i className="fa icon-ETLBatch"/>
                  <span>{T.translate('features.Wizard.HydratorPipeline.batchLinkLabel')}</span>
                </a>
                <a
                  href={this.state.realtimePipelineUrl}
                  onClick={this.closeModal.bind(this)}
                  className="btn btn-secondary"
                >
                  <i className="fa icon-sparkstreaming"/>
                  <span>{T.translate('features.Wizard.HydratorPipeline.realtimeLinkLabel')}</span>
                </a>
              </div>
            </div>
          </ReactCSSTransitionGroup>
        </ModalBody>
      </Modal>
    );

  }
}

HydratorPipeline.propTypes = {
  isOpen: PropTypes.bool,
  onClose: PropTypes.func
};
