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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import {MyMarketApi} from 'api/market';
import T from 'i18n-react';
import AbstractWizard from 'components/AbstractWizard';
import classnames from 'classnames';
import uuidV4 from 'uuid/v4';
import moment from 'moment';
import getIcon from 'services/market-action-icon-map';

require('./MarketEntityModal.scss');

export default class MarketEntityModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entityDetail: {},
      wizard: {
        actionIndex: null,
        actionType: null,
        action: null
      },
      completedActions: []
    };
  }

  componentWillMount() {
    MyMarketApi.get({
      packageName: this.props.entity.name,
      version: this.props.entity.version
    }).subscribe((res) => {
      this.setState({entityDetail: res});
    }, (err) => {
      console.log('Error', err);
    });
  }
  closeWizard(returnResult) {
    if (returnResult) {
      this.setState({
        completedActions: this.state.completedActions.concat([this.state.wizard.actionIndex]),
        wizard: {
          actionIndex: null,
          actionType: null,
          action: null
        }
      });
      return;
    }
    this.setState({
      wizard: {
        actionIndex: null,
        actionType: null
      }
    });
  }
  openWizard(actionIndex, actionType, action) {
    this.setState({
      wizard: {
        actionIndex,
        actionType,
        action
      }
    });
  }

  getVersion() {
    const versionElem = (
      <span>
        <strong>
          {this.state.entityDetail.cdapVersion}
        </strong>
      </span>
    );

    return this.state.entityDetail.cdapVersion ? versionElem : null;
  }

  render() {
    let actions;
    if (this.state.entityDetail.actions) {
      actions = (
        <div className="market-entity-actions">
          {
            this.state.entityDetail.actions.map((action, index) => {
              let isCompletedAction = this.state.completedActions.indexOf(index) !== -1;
              let actionName = T.translate('features.Market.action-types.' + action.type + '.name');
              let actionIcon = getIcon(action.type);
              return (
                <div
                  className="action-container text-xs-center"
                  key={uuidV4()}
                  onClick={this.openWizard.bind(this, index, action.type, action)}
                >
                  <div
                    className="action"
                    key={index}
                  >
                    <div className="step text-xs-center">
                      <span className={classnames("tag tag-pill", {'completed' : isCompletedAction})}>{index + 1}</span>
                    </div>
                    <div className="action-icon">
                      <div className={classnames("fa", actionIcon)}></div>
                    </div>
                    <div className="action-description">
                      {action.label}
                    </div>
                    <button
                      className={classnames("btn btn-link", {'btn-completed': isCompletedAction})}
                    >
                      { actionName }
                    </button>
                  </div>
                </div>
              );
            })
          }
        </div>
      );
    }

    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.onCloseHandler.bind(this)}
        className="market-entity-modal"
        size="md"
      >
        <ModalHeader toggle={this.props.onCloseHandler.bind(this)}>
          <span className="float-xs-left">
            { this.props.entity.label }
          </span>
          <span className="version float-xs-right">
            <span className="version-text">
              {T.translate('features.MarketEntityModal.version')}
            </span>
            <span className="version-number">{ this.props.entity.version }</span>
            <span
              className="fa fa-times"
              onClick={this.props.onCloseHandler.bind(this)}
            >
            </span>
          </span>
        </ModalHeader>
        <ModalBody>
          <div className="entity-information">
            <div className="entity-modal-image">
              <img src={MyMarketApi.getIcon(this.props.entity)} />
            </div>
            <div className="entity-content">
              <div className="entity-description">
                {this.state.entityDetail.description}
              </div>
              <div className="entity-metadata">
                <div>Author</div>
                <span>
                  <strong>
                    {this.state.entityDetail.author}
                  </strong>
                </span>
                <div>Company</div>
                <span>
                  <strong>
                    {this.state.entityDetail.org}
                  </strong>
                </span>
                <div>Created</div>
                <span>
                  <strong>
                    {(moment(this.state.entityDetail.created * 1000)).format('MM-DD-YYYY HH:mm A')}
                  </strong>
                </span>
                {this.state.entityDetail.cdapVersion ? <div>CDAP Version</div> : null}
                {this.getVersion()}
              </div>
            </div>
          </div>

          {actions}

          <AbstractWizard
            isOpen={this.state.wizard.actionIndex !== null && this.state.wizard.actionType !== null}
            onClose={this.closeWizard.bind(this)}
            wizardType={this.state.wizard.actionType}
            input={{action: this.state.wizard.action, package: this.props.entity}}
          />
        </ModalBody>
      </Modal>
    );

  }
}

MarketEntityModal.propTypes = {
  isOpen: PropTypes.bool,
  onCloseHandler: PropTypes.func,
  entity: PropTypes.object
};
