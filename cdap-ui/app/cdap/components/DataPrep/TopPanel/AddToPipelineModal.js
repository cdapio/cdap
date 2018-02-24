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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import T from 'i18n-react';
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import CardActionFeedback from 'components/CardActionFeedback';
import getPipelineConfig from 'components/DataPrep/TopPanel/PipelineConfigHelper';

const mapErrorToMessage = (message) => {
  if (message.indexOf('invalid field name') !== -1) {
    let splitMessage = message.split("field name: ");
    let fieldName = objectQuery(splitMessage, 1) || message;
    return {
      message: T.translate(`${PREFIX}.invalidFieldNameMessage`, {fieldName}),
      remedies: `${T.translate(`${PREFIX}.invalidFieldNameRemedies1`)}`
    };
  }
  return {message};
};

const PREFIX = 'features.DataPrep.TopPanel';
export default class AddToHydratorModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
      batchUrl: null,
      realtimeUrl: null,
      error: null,
      workspaceId: null,
      realtimeConfig: null,
      batchConfig: null
    };
  }

  componentWillMount() {
    this.generateLinks();
  }

  generateLinks() {
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId;
    let namespace = getCurrentNamespace();

    getPipelineConfig().subscribe(
      (res) => {
        let realtimeUrl;

        if (!res.realtimeConfig) {
          realtimeUrl = null;
        } else {
          realtimeUrl = window.getHydratorUrl({
            stateName: 'hydrator.create',
            stateParams: {
              namespace,
              workspaceId,
              artifactType: 'cdap-data-streams'
            }
          });
        }

        let batchUrl = window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
            workspaceId,
            artifactType: 'cdap-data-pipeline'
          }
        });

        this.setState({
          loading: false,
          realtimeUrl,
          batchUrl,
          workspaceId,
          realtimeConfig: res.realtimeConfig,
          batchConfig: res.batchConfig
        });
      },
      (err) => {
        let {message, remedies = null} = mapErrorToMessage(err);

        if (remedies) {
          this.setState({
            error: {message, remedies},
            loading: false
          });
          return;
        }

        this.setState({
          error: err,
          loading: false
        });
      }
    );
  }

  applyDirective(directive) {
    execute([directive])
      .subscribe(
        () => {
          this.setState({
            error: null,
            loading: true,
            schema: []
          }, () => {
            this.generateLinks();
          });
        },
        (err) => {
          console.log('Error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }

  renderInvalidFieldError() {
    return (
      <div className="message">
        <pre>
          <div className="remedy-message">
            {
              objectQuery(this.state, 'error', 'remedies') ? this.state.error.remedies : null
            }
          </div>
          <span>
            {T.translate(`${PREFIX}.invalidFieldNameRemedies2`)}
            <span
              className="btn-link"
              onClick={this.applyDirective.bind(this, 'cleanse-column-names')}
            >
              {T.translate(`${PREFIX}.cleanseLinkLabel`)}
            </span>
            {T.translate(`${PREFIX}.invalidFieldNameRemedies3`)}
          </span>
        </pre>
      </div>
    );
  }

  render() {
    let content;

    if (this.state.loading) {
      content = (
        <div className="loading-container">
          <h4 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    } else {
      let realtimeDisabledTooltip;
      let type = DataPrepStore.getState().dataprep.workspaceInfo.properties.connection;

      if (!this.state.realtimeUrl) {
        realtimeDisabledTooltip = T.translate(`${PREFIX}.realtimeDisabledTooltip`, {
          type: T.translate(`${PREFIX}.${type}`)
        });
      }

      content = (
        <div>
          <div className="message">
            {T.translate(`${PREFIX}.addToPipelineModal.title`)}
          </div>
          <div className="action-buttons">
            <a
              href={this.state.error ? null : this.state.batchUrl}
              className={classnames('btn btn-secondary', {
                'inactive': this.state.error
              })}
              onClick={(() => {
                if (this.state.error) { return; }
                window.localStorage.setItem(this.state.workspaceId, JSON.stringify(this.state.batchConfig));
              }).bind(this)}
            >
              <i className="fa icon-ETLBatch"/>
              <span>{T.translate(`${PREFIX}.addToPipelineModal.batchPipelineBtn`)}</span>
            </a>
            <a
              href={this.state.realtimeUrl}
              className={classnames('btn btn-secondary', {
                'inactive': !this.state.realtimeUrl || this.state.error
              })}
              onClick={(() => {
                if (!this.state.realtimeUrl) { return; }
                window.localStorage.setItem(this.state.workspaceId, JSON.stringify(this.state.realtimeConfig));
              }).bind(this)}
              title={realtimeDisabledTooltip}
            >
              <i className="fa icon-sparkstreaming"/>
              <span>{T.translate(`${PREFIX}.addToPipelineModal.realtimePipelineBtn`)}</span>
            </a>
          </div>
        </div>
      );
    }

    let showContent = !objectQuery(this.state, 'error', 'remedies');

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="lg"
        className="add-to-pipeline-dataprep-modal"
      >
        <ModalHeader>
          <span>
            {T.translate(`${PREFIX}.addToPipelineBtnLabel`)}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          {showContent ? content : this.renderInvalidFieldError()}

        </ModalBody>
        {
          this.state.error && showContent ?
            <ModalFooter className="dataset-copy-error-container">
              <CardActionFeedback
                type='DANGER'
                message={T.translate(`${PREFIX}.addToPipelineModal.errorTitle`)}
                extendedMessage={this.state.error}
              />
            </ModalFooter>
          :
            null
        }
      </Modal>
    );
  }
}

AddToHydratorModal.propTypes = {
  toggle: PropTypes.func
};
