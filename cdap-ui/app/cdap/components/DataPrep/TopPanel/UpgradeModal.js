/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, { Component, PropTypes } from 'react';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import enableDataPreparationService from 'components/DataPrep/DataPrepServiceControl/ServiceEnablerUtilities';
import CardActionFeedback from 'components/CardActionFeedback';
import ee from 'event-emitter';
import {i18nPrefix, MIN_DATAPREP_VERSION, artifactName} from 'components/DataPrep';
import MyDataPrepApi from 'api/dataprep';

export default class UpgradeModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: false,
      error: null,
      extendedMessage: null
    };

    this.eventEmitter = ee(ee);
    this.attemptClose = this.attemptClose.bind(this);
    this.upgradeClick = this.upgradeClick.bind(this);
  }

  attemptClose() {
    if (this.state.loading) { return; }

    this.props.toggle();
  }

  upgradeClick() {
    this.setState({loading: true});

    enableDataPreparationService({
      shouldStopService: true,
      artifactName,
      api: MyDataPrepApi,
      i18nPrefix,
      MIN_VERSION: MIN_DATAPREP_VERSION
    })
      .subscribe(() => {
        this.eventEmitter.emit('REFRESH_DATAPREP');
      }, (err) => {
        this.setState({
          loading: false,
          error: err.error,
          extendedMessage: err.extendedMessage
        });
      });
  }

  renderError() {
    if (!this.state.error) { return null; }

    return (
      <CardActionFeedback
        type="DANGER"
        message={this.state.error}
        extendedMessage={this.state.extendedMessage}
      />
    );
  }

  render() {
    let content;

    if (this.state.loading) {
      content = (
        <div className="loading-container">
          <h3 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h3>
        </div>
      );
    } else {
      content = (
        <div>
          <div className="message">
            Are you sure you want to upgrade Data Preparation?
          </div>

          <div className="action-buttons">
            <div className="action-buttons">
              <button
                className="btn btn-secondary"
                onClick={this.upgradeClick}
              >
                Yes
              </button>
              <button
                className="btn btn-secondary"
                onClick={this.attemptClose}
              >
                No
              </button>
            </div>
          </div>
        </div>
      );
    }

    return (
      <Modal
        isOpen={true}
        toggle={this.attemptClose}
        className="dataprep-upgrade-modal"
        zIndex="1061"
      >
        <ModalHeader>
          <span>
            Upgrade Data Preparation
          </span>
          {
            this.state.loading ? null : (
              <div
                className="close-section float-xs-right"
              >
                <span
                  className="fa fa-times"
                  onClick={this.attemptClose}
                />
              </div>
            )
          }
        </ModalHeader>
        <ModalBody>
          {content}
          {this.renderError()}
        </ModalBody>
      </Modal>
    );
  }
}

UpgradeModal.propTypes = {
  toggle: PropTypes.func
};
