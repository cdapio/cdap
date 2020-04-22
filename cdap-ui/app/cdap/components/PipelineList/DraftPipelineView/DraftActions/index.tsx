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
import T from 'i18n-react';
import { deleteDraft } from 'components/PipelineList/DraftPipelineView/store/ActionCreator';
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';
import ActionsPopover, { IAction } from 'components/ActionsPopover';
import PipelineExportModal from 'components/PipelineExportModal';
import ConfirmationModal from 'components/ConfirmationModal';
import downloadFile from 'services/download-file';

const PREFIX = 'features.PipelineList.DeleteConfirmation';

interface IProps {
  draft: IDraft;
}

interface IState {
  showExport: boolean;
  showDeleteConfirmation: boolean;
  showPopover?: boolean;
}

class DraftActions extends React.PureComponent<IProps, IState> {
  public state = {
    showExport: false,
    showDeleteConfirmation: false,
    showPopover: false,
  };

  public pipelineConfig = {};

  private handlePipelineExport = () => {
    const draft = this.props.draft;

    this.pipelineConfig = {
      name: draft.name,
      description: draft.description,
      artifact: draft.artifact,
      config: draft.config,
    };

    if (window.Cypress) {
      this.openExportModal();
      return;
    }
    const postExportCb = () => {
      this.pipelineConfig = {};
      this.setState({ showPopover: false });
    };

    // Unless we are running an e2e test, just export the pipeline JSON
    downloadFile(this.pipelineConfig, postExportCb);
  };

  private openExportModal = (): void => {
    this.setState({
      showExport: true,
      showPopover: false,
    });
  };

  private closeExportModal = (): void => {
    this.pipelineConfig = {};

    this.setState({
      showExport: false,
    });
  };

  private toggleDeleteConfirmation = () => {
    this.setState({
      showDeleteConfirmation: !this.state.showDeleteConfirmation,
      showPopover: false,
    });
  };

  private togglePopover = () => {
    this.setState({ showPopover: !this.state.showPopover });
  };

  private renderConfirmationBody = () => {
    return (
      <div>
        {T.translate(`${PREFIX}.confirmDraftPrompt`)}
        <strong>
          <em>{this.props.draft.name}</em>
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
        headerTitle={T.translate(`${PREFIX}.titleDraft`)}
        toggleModal={this.toggleDeleteConfirmation}
        confirmationElem={this.renderConfirmationBody()}
        confirmButtonText={T.translate(`${PREFIX}.confirm`)}
        confirmFn={deleteDraft.bind(null, this.props.draft)}
        cancelFn={this.toggleDeleteConfirmation}
        isOpen={this.state.showDeleteConfirmation}
      />
    );
  };

  private actions: IAction[] = [
    {
      label: T.translate('commons.export'),
      actionFn: this.handlePipelineExport,
    },
    {
      label: 'separator',
    },
    {
      label: T.translate('commons.delete'),
      actionFn: this.toggleDeleteConfirmation,
      className: 'delete',
    },
  ];

  public render() {
    return (
      <div className="action" onClick={(e) => e.preventDefault()}>
        <ActionsPopover
          actions={this.actions}
          showPopover={this.state.showPopover}
          togglePopover={this.togglePopover}
        />

        <PipelineExportModal
          isOpen={this.state.showExport}
          onClose={this.closeExportModal}
          pipelineConfig={this.pipelineConfig}
          onExport={this.handlePipelineExport}
        />

        {this.renderDeleteConfirmation()}
      </div>
    );
  }
}

export default DraftActions;
