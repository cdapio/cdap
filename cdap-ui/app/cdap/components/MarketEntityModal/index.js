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

require('./MarketEntityModal.less');

export default class MarketEntityModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entityDetail: {}
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

  render() {
    let actions;

    if (this.state.entityDetail.actions) {
      actions = (
        <div className="market-entity-actions text-center">
          {
            this.state.entityDetail.actions.map((action, index) => {
              return (
                <div
                  className="action"
                  key={index}
                >
                  <div className="action-image">
                  </div>

                  <div className="action-description text-center">
                    <span>{index + 1}. </span>
                    {action.type}
                  </div>
                  <button className="btn btn-text">
                    Add
                  </button>
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
          <span className="pull-right">
            <span>{T.translate('features.MarketEntityModal.version')}</span>
            { this.props.entity.version }
          </span>
        </ModalHeader>
        <ModalBody>
          <div className="entity-information">
            <div className="entity-modal-image">
              <img src={MyMarketApi.getIcon(this.props.entity)} />
            </div>
            <div className="entity-description">
              {this.state.entityDetail.description}
            </div>
          </div>

          <hr />

          {actions}

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
