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
import {MyMarketApi} from '../../api/market';
import T from 'i18n-react';
import AbstractWizard from 'components/AbstractWizard';
import classnames from 'classnames';
import shortid from 'shortid';

require('./MarketEntityModal.less');

export default class MarketEntityModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entityDetail: {},
      wizard: {
        actionIndex: null,
        actionType: null
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
          actionType: null
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
  openWizard(actionIndex, actionType) {
    this.setState({
      wizard: {
        actionIndex,
        actionType
      }
    });
  }

  render() {
    let actions;

    if (this.state.entityDetail.actions) {
      actions = (
        <div className="market-entity-actions">
          {
            this.state.entityDetail.actions.map((action, index) => {
              let isCompletedAction = this.state.completedActions.indexOf(index) !== -1 ;
              let actionName = T.translate('features.Market.action-types.' + action.type + '.name');
              let actionIcon = T.translate('features.Market.action-types.' + action.type + '.icon');
              return (
                <div className="action-container text-center" key={shortid.generate()}>
                  <div className={classnames("fa fa-check-circle text-success", {show: isCompletedAction})}></div>
                  <div
                    className="action"
                    key={index}
                  >
                    <div>Step {index + 1} </div>
                    <div className={classnames("fa", actionIcon)}></div>
                    <div className="action-description"></div>
                    <button
                      className={classnames("btn btn-default", {'btn-completed': isCompletedAction})}
                      onClick={this.openWizard.bind(this, index, action.type)}
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
        <ModalHeader>
          <span className="pull-left">
            { this.props.entity.label }
          </span>
          <span className="version pull-right">
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
              {actions}
            </div>
          </div>
          <AbstractWizard
            isOpen={this.state.wizard.actionIndex !== null && this.state.wizard.actionType !== null}
            onClose={this.closeWizard.bind(this)}
            wizardType={this.state.wizard.actionType}
            context={this.props.entity.label}
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
