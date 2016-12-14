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

import React, { Component, PropTypes } from 'react';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import T from 'i18n-react';

require('./AddToHydrator.less');

export default class AddToHydrator extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      batchUrl: '',
      realtimeUrl: '',
      loading: false
    };

    this.toggle = this.toggle.bind(this);
    this.onAddToHydratorClick = this.onAddToHydratorClick.bind(this);
  }

  toggle() {
    this.setState({isOpen: !this.state.isOpen});
  }

  onAddToHydratorClick() {
    let linkGenerator$ = this.props.linkGenerator();

    linkGenerator$.subscribe((res) => {
      this.setState({
        batchUrl: res.batchUrl,
        realtimeUrl: res.realtimeUrl,
        loading: false
      });
    });

    this.setState({isOpen: true, loading: true});
  }

  render() {
    const loading = (
      <div className="loading">
        <h3 className="text-center">
          <span className="fa fa-spinner fa-spin" />
        </h3>
      </div>
    );

    return (
      <span>
        <button
          className="btn btn-primary"
          onClick={this.onAddToHydratorClick}
        >
          <span className="fa icon-hydrator" />
          Add to Hydrator
        </button>

        <Modal
          isOpen={this.state.isOpen}
          toggle={this.toggle}
          className="wrangler-hydrator-pipeline-modal"
          size="lg"
        >
          <ModalHeader>
            <span className="pull-left">
              {T.translate('features.Wizard.HydratorPipeline.title')}
            </span>
            <div className="close-section pull-right">
              <span
                className="fa fa-times"
                onClick={this.toggle}
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
              {
                this.state.loading ?
                  loading
                    :
                  (
                  <div className="hydrator-pipeline-content-container">
                    <div className="message">
                      {T.translate('features.Wizard.HydratorPipeline.message')}
                    </div>
                    <div className="action-buttons">
                      <a
                        href={this.state.batchUrl}
                        className="btn btn-default"
                      >
                        <i className="fa icon-ETLBatch"/>
                        <span>{T.translate('features.Wizard.HydratorPipeline.batchLinkLabel')}</span>
                      </a>
                      <a
                        href={this.state.realtimeUrl}
                        className="btn btn-default"
                      >
                        <i className="fa icon-sparkstreaming"/>
                        <span>{T.translate('features.Wizard.HydratorPipeline.realtimeLinkLabel')}</span>
                      </a>
                    </div>
                  </div>
                )
              }
            </ReactCSSTransitionGroup>
          </ModalBody>
        </Modal>
      </span>
    );
  }
}

AddToHydrator.propTypes = {
  linkGenerator: PropTypes.func
};
