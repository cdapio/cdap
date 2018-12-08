/*
 * Copyright © 2018 Cask Data, Inc.
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
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import ConfirmationModal from 'components/ConfirmationModal';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyAppApi } from 'api/app';
import PipelineExportModal from 'components/PipelineExportModal';
import TriggeredPipelineStore from 'components/TriggeredPipelines/store/TriggeredPipelineStore';
import T from 'i18n-react';
import classnames from 'classnames';
import { duplicatePipeline } from 'services/PipelineUtils';
require('./PipelineDetailsActionsButton.scss');

const PREFIX = 'features.PipelineDetails.TopPanel';

export default class PipelineDetailsActionsButton extends Component {
  static propTypes = {
    pipelineName: PropTypes.string,
    description: PropTypes.string,
    artifact: PropTypes.object,
    config: PropTypes.object,
  };

  state = {
    showExportModal: false,
    showDeleteConfirmationModal: false,
    showPopover: false,
  };

  togglePopover = (showPopover = !this.state.showPopover) => {
    this.setState({
      showPopover,
    });
  };

  componentWillReceiveProps(nextProps) {
    this.pipelineConfig = {
      ...this.pipelineConfig,
      config: nextProps.config,
    };
  }

  pipelineConfig = {
    name: this.props.pipelineName,
    description: this.props.description,
    artifact: this.props.artifact,
    config: this.props.config,
  };

  duplicateConfigAndNavigate = () => {
    duplicatePipeline(this.props.pipelineName, this.pipelineConfig);
  };

  deletePipeline = () => {
    let namespace = getCurrentNamespace();
    let params = {
      namespace,
      appId: this.props.pipelineName,
    };
    const pipelinesListLink = window.getHydratorUrl({
      stateName: 'hydrator.list',
      stateParams: {
        namespace,
      },
    });

    MyAppApi.delete(params).subscribe(
      () => {
        this.setState({
          deleteErrMsg: '',
          extendedDeleteErrMsg: '',
        });
        window.location.href = pipelinesListLink;
      },
      (err) => {
        this.setState({
          deleteErrMsg: T.translate(`${PREFIX}.deleteError`),
          extendedDeleteErrMsg: err,
        });
      }
    );
  };

  toggleExportModal = () => {
    this.setState({ showExportModal: !this.state.showExportModal });
  };

  toggleDeleteConfirmationModal = () => {
    this.setState({
      showDeleteConfirmationModal: !this.state.showDeleteConfirmationModal,
      deleteErrMsg: '',
      extendedDeleteErrMsg: '',
    });
  };

  renderExportPipelineModal() {
    if (!this.state.showExportModal) {
      return null;
    }

    return (
      <PipelineExportModal
        isOpen={this.state.showExportModal}
        onClose={this.toggleExportModal}
        pipelineConfig={this.pipelineConfig}
      />
    );
  }

  getDeleteConfirmationElem = () => {
    let triggeredPipelines = TriggeredPipelineStore.getState().triggered.triggeredPipelines;
    let count = triggeredPipelines.length;

    if (count > 0) {
      let triggersText = triggeredPipelines.map((pipeline) => pipeline.application).join(', ');

      return (
        <div>
          {T.translate(`${PREFIX}.deleteConfirmation.pipeline`)}
          <strong>
            <em>{this.props.pipelineName}</em>
          </strong>
          {T.translate(`${PREFIX}.deleteConfirmation.trigger`)}
          {T.translate(`${PREFIX}.deleteConfirmation.triggerPluralCheck`, { context: count })}
          <em>{triggersText}</em>
          {T.translate(`${PREFIX}.deleteConfirmation.triggerDelete`)}
          <strong>
            <em>{this.props.pipelineName}</em>
          </strong>
          {T.translate(`${PREFIX}.deleteConfirmation.proceedPrompt`)}
        </div>
      );
    }

    return (
      <div>
        {T.translate(`${PREFIX}.deleteConfirmation.confirmPrompt`)}
        <strong>
          <em>{this.props.pipelineName}</em>
        </strong>
        ?
      </div>
    );
  };

  renderDeleteConfirmationModal() {
    if (!this.state.showDeleteConfirmationModal) {
      return null;
    }

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.deleteConfirmation.title`)}
        toggleModal={this.toggleDeleteConfirmationModal}
        confirmationElem={this.getDeleteConfirmationElem()}
        confirmButtonText={T.translate(`${PREFIX}.deleteConfirmation.confirm`)}
        confirmFn={this.deletePipeline}
        cancelFn={this.toggleDeleteConfirmationModal}
        isOpen={this.state.showDeleteConfirmationModal}
        isLoading={this.state.loading}
        errorMessage={this.state.deleteErrMsg}
        extendedMessage={this.state.extendedDeleteErrMsg}
      />
    );
  }

  render() {
    const ActionsBtnAndLabel = () => {
      return (
        <div className="btn pipeline-action-btn pipeline-actions-btn" onClick={this.togglePopover}>
          <div className="btn-container">
            <IconSVG name="icon-cog-empty" />
            <div className="button-label">{T.translate(`${PREFIX}.actions`)}</div>
          </div>
        </div>
      );
    };

    return (
      <div
        className={classnames('pipeline-action-container pipeline-actions-container', {
          active: this.state.showPopover,
        })}
      >
        <Popover
          target={ActionsBtnAndLabel}
          placement="bottom"
          bubbleEvent={false}
          enableInteractionInPopover={true}
          className="pipeline-actions-popper"
          data-testid="pipeline-actions-popper"
          showPopover={this.state.showPopover}
          onTogglePopover={this.togglePopover}
        >
          <ul>
            <li onClick={this.duplicateConfigAndNavigate}>{T.translate(`${PREFIX}.duplicate`)}</li>
            <li onClick={this.toggleExportModal}>{T.translate(`${PREFIX}.export`)}</li>
            <hr />
            <li
              onClick={this.toggleDeleteConfirmationModal}
              className="delete-action"
              data-testid="delete-pipeline"
            >
              {T.translate('commons.delete')}
            </li>
          </ul>
        </Popover>
        {this.renderExportPipelineModal()}
        {this.renderDeleteConfirmationModal()}
      </div>
    );
  }
}
