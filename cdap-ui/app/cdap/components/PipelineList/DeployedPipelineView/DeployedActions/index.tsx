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

import * as React from 'react';
import { deletePipeline } from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
import { Actions } from 'components/PipelineList/DeployedPipelineView/store';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import ActionsPopover, { IAction } from 'components/ActionsPopover';
import { duplicatePipeline, getPipelineConfig } from 'services/PipelineUtils';
import PipelineExportModal from 'components/PipelineExportModal';
import ConfirmationModal from 'components/ConfirmationModal';
import { connect } from 'react-redux';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyScheduleApi } from 'api/schedule';
import { GLOBALS } from 'services/global-constants';
import T from 'i18n-react';

const PREFIX = 'features.PipelineList.DeleteConfirmation';

interface IProps {
  pipeline: IPipeline;
  deleteError?: string;
  clearDeleteError: () => void;
}

interface ITriggeredPipeline {
  application: string;
}

interface IState {
  showExport: boolean;
  showDeleteConfirmation: boolean;
  triggeredPipelines: ITriggeredPipeline[];
}

class DeployedActionsView extends React.PureComponent<IProps, IState> {
  public state = {
    showExport: false,
    showDeleteConfirmation: false,
    triggeredPipelines: [],
  };

  private pipelineConfig = {};

  private showExportModal = () => {
    getPipelineConfig(this.props.pipeline.name).subscribe((pipelineConfig) => {
      this.pipelineConfig = pipelineConfig;

      this.setState({
        showExport: true,
      });
    });
  };

  private closeExportModal = () => {
    this.pipelineConfig = {};

    this.setState({
      showExport: false,
    });
  };

  private renderConfirmationBody = () => {
    const triggered = this.state.triggeredPipelines;

    if (triggered.length > 0) {
      const triggersText = this.state.triggeredPipelines
        .map((pipeline) => pipeline.application)
        .join(', ');

      return (
        <div>
          {T.translate(`${PREFIX}.pipeline`)}
          <strong>
            <em>{this.props.pipeline.name}</em>
          </strong>
          {T.translate(`${PREFIX}.trigger`)}
          {T.translate(`${PREFIX}.triggerPluralCheck`, {
            context: triggered.length,
          })}
          <em>{triggersText}</em>
          {T.translate(`${PREFIX}.triggerDelete`)}
          <strong>
            <em>{this.props.pipeline.name}</em>
          </strong>
          {T.translate(`${PREFIX}.proceedPrompt`)}
        </div>
      );
    }

    return (
      <div>
        {T.translate(`${PREFIX}.confirmPrompt`)}
        <strong>
          <em>{this.props.pipeline.name}</em>
        </strong>
        ?
      </div>
    );
  };

  private renderDeleteConfirmation = () => {
    if (!this.state.showDeleteConfirmation) {
      return null;
    }

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.title`)}
        toggleModal={this.toggleDeleteConfirmation}
        confirmationElem={this.renderConfirmationBody()}
        confirmButtonText={T.translate(`${PREFIX}.confirm`)}
        confirmFn={deletePipeline.bind(null, this.props.pipeline)}
        cancelFn={this.toggleDeleteConfirmation}
        isOpen={this.state.showDeleteConfirmation}
        errorMessage={!this.props.deleteError ? '' : T.translate(`${PREFIX}.deleteError`)}
        extendedMessage={this.props.deleteError}
      />
    );
  };

  /**
   * If the pipeline is a batch pipeline, we have to fetch the list of schedules first. Otherwise
   * just show the confirmation modal.
   */
  private showDeleteConfirmation = () => {
    const pipeline = this.props.pipeline;

    if (pipeline.artifact.name !== GLOBALS.etlDataPipeline) {
      this.setState({
        showDeleteConfirmation: true,
        triggeredPipelines: [],
      });
      return;
    }

    const namespace = getCurrentNamespace();
    const params = {
      namespace,
      'trigger-namespace-id': namespace,
      'trigger-program-type': 'workflows',
      'trigger-app-name': this.props.pipeline.name,
      'trigger-program-name': 'DataPipelineWorkflow',
      'schedule-status': 'SCHEDULED',
    };

    MyScheduleApi.getTriggeredList(params).subscribe((res) => {
      this.setState({
        showDeleteConfirmation: true,
        triggeredPipelines: res,
      });
    });
  };

  private toggleDeleteConfirmation = () => {
    this.setState({
      showDeleteConfirmation: !this.state.showDeleteConfirmation,
      triggeredPipelines: [],
    });

    if (!this.state.showDeleteConfirmation) {
      this.props.clearDeleteError();
    }
  };

  private actions: IAction[] = [
    {
      label: T.translate('commons.duplicate'),
      actionFn: duplicatePipeline.bind(null, this.props.pipeline.name),
    },
    {
      label: T.translate('commons.export'),
      actionFn: this.showExportModal,
    },
    {
      label: 'separator',
    },
    {
      label: T.translate('commons.delete'),
      actionFn: this.showDeleteConfirmation,
      className: 'delete',
    },
  ];

  public render() {
    return (
      <div className="action">
        <span onClick={(e) => e.preventDefault()}>
          <ActionsPopover actions={this.actions} />
        </span>

        <PipelineExportModal
          isOpen={this.state.showExport}
          onClose={this.closeExportModal}
          pipelineConfig={this.pipelineConfig}
        />

        {this.renderDeleteConfirmation()}
      </div>
    );
  }
}

const mapStateToProps = (state, ownProp) => {
  return {
    deleteError: state.deployed.deleteError,
    pipeline: ownProp.pipeline,
  };
};

const mapDispatch = (dispatch) => {
  return {
    clearDeleteError: () => {
      dispatch({
        type: Actions.clearDeleteError,
      });
    },
  };
};

const DeployedActions = connect(
  mapStateToProps,
  mapDispatch
)(DeployedActionsView);

export default DeployedActions;
