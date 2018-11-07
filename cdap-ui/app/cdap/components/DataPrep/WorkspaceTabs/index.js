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
import DataPrepStore from 'components/DataPrep/store';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import { getWorkspaceList } from 'components/DataPrep/store/DataPrepActionCreator';
import WorkspaceTab from 'components/DataPrep/WorkspaceTabs/WorkspaceTab';
import UncontrolledPopover from 'components/UncontrolledComponents/Popover';
import { Link } from 'react-router-dom';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import ConfirmationModal from 'components/ConfirmationModal';
import T from 'i18n-react';
import findIndex from 'lodash/findIndex';
import debounce from 'lodash/debounce';
import MouseTrap from 'mousetrap';

require('./WorkspaceTabs.scss');

const WORKSPACE_WIDTH = 200;
const INITIAL_MAX_TABS = 5;
const PREFIX = 'features.DataPrep.WorkspaceTabs';

export default class WorkspaceTabs extends Component {
  constructor(props) {
    super(props);

    let initialState = DataPrepStore.getState();

    let initialSplit = this.splitTabs(
      initialState.workspaces.list,
      INITIAL_MAX_TABS,
      initialState.dataprep.workspaceId
    );

    this.state = {
      activeWorkspace: initialState.dataprep.workspaceId,
      workspaceList: initialState.workspaces.list,
      maxTabs: INITIAL_MAX_TABS,
      deleteWorkspace: null,
      displayTabs: initialSplit.displayTabs,
      dropdownTabs: initialSplit.dropdownTabs,
      sidePanelToggle: this.props.sidePanelToggle,
    };

    this.namespace = NamespaceStore.getState().selectedNamespace;

    this.getWorkspaceList = this.getWorkspaceList.bind(this);
    this.splitTabs = this.splitTabs.bind(this);
    this.debouncedCalculateMaxTabs = debounce(this.calculateMaxTabs.bind(this), 300);
    this.shouldUpdate = false;

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState();

      this.setState({
        activeWorkspace: state.dataprep.workspaceId,
        workspaceList: state.workspaces.list,
      });
      this.calculateMaxTabs();
    });
  }

  componentDidMount() {
    this.calculateMaxTabs();
    MouseTrap.bind('enter', () => {
      if (this.state.deleteWorkspace) {
        this.handleDeleteWorkspace(this.state.deleteWorkspace.id);
      }
    });

    window.addEventListener('resize', this.debouncedCalculateMaxTabs);
  }

  componentDidUpdate() {
    if (this.shouldUpdate) {
      this.shouldUpdate = false;
      this.calculateMaxTabs();
    }
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.sidePanelToggle !== nextProps.sidePanelToggle) {
      this.shouldUpdate = true;
    }
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
    MouseTrap.unbind('enter');
    window.removeEventListener('resize', this.debouncedCalculateMaxTabs);
  }

  splitTabs(workspaceList, maxTabs, activeWorkspaceId = this.state.activeWorkspace) {
    let displayTabs = workspaceList.slice(0, maxTabs);
    let dropdownTabs = workspaceList.slice(maxTabs);

    let activeWorkspaceIndex = findIndex(dropdownTabs, { id: activeWorkspaceId });

    if (activeWorkspaceIndex !== -1) {
      let activeWorkspace = dropdownTabs.splice(activeWorkspaceIndex, 1);
      let lastWorkspaceFromDisplayedTabs = displayTabs.pop();

      displayTabs.push(activeWorkspace[0]);
      // If the workspace screen is small enough, it's possible that maxTabs will
      // be 0, causing displayTabs to be an empty list. In that case,
      // lastWorkspaceFromDisplayedTabs will be undefined. Therefore we need
      // to check here if the variable is not undefined, otherwise the UI
      // will break
      if (lastWorkspaceFromDisplayedTabs) {
        dropdownTabs.unshift(lastWorkspaceFromDisplayedTabs);
      }
    }

    return {
      displayTabs,
      dropdownTabs,
    };
  }

  calculateMaxTabs() {
    let containerElem = document.getElementsByClassName('workspace-tabs')[0];
    let boundingBox = containerElem.getBoundingClientRect();

    let maxTabs = Math.floor((boundingBox.width - 200) / WORKSPACE_WIDTH);

    let { displayTabs, dropdownTabs } = this.splitTabs(this.state.workspaceList, maxTabs);

    this.setState({
      maxTabs,
      displayTabs,
      dropdownTabs,
    });
  }

  getWorkspaceList() {
    getWorkspaceList();
  }

  handleDeleteWorkspace(workspaceId) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.delete({
      namespace,
      workspaceId,
    }).subscribe(
      () => {
        if (this.props.onWorkspaceDelete) {
          this.props.onWorkspaceDelete();
        }

        this.setState({ deleteWorkspace: null });
        this.getWorkspaceList();
      },
      (err) => {
        console.log('Error Deleting', err);
      }
    );
  }

  toggleDeleteWorkspace = (workspace) => {
    this.setState({ deleteWorkspace: workspace });
  };

  renderDropdown() {
    if (this.state.workspaceList.length <= this.state.maxTabs) {
      return null;
    }

    let list = this.state.dropdownTabs;

    return (
      <div className="workspace-tab workspace-dropdown text-center">
        <UncontrolledPopover popoverClassName="workspace-list-popover">
          {list.map((workspace) => {
            return (
              <div key={workspace.id} className="workspace-list-dropdown-item">
                <Link
                  to={`/ns/${this.namespace}/dataprep/${workspace.id}`}
                  className={classnames('workspace-link', {
                    active: this.state.activeWorkspace === workspace.id,
                  })}
                >
                  {workspace.name}
                </Link>

                <span
                  className="fa float-right"
                  onClick={this.toggleDeleteWorkspace.bind(this, workspace)}
                >
                  <IconSVG name="icon-close" />
                </span>
              </div>
            );
          })}
        </UncontrolledPopover>
      </div>
    );
  }

  renderWorkspaceTabs() {
    let displayWorkspace = this.state.displayTabs;

    return (
      <div className="workspace-tabs-list">
        {displayWorkspace.map((workspace) => {
          return (
            <WorkspaceTab
              workspace={workspace}
              active={this.state.activeWorkspace === workspace.id}
              onDelete={this.toggleDeleteWorkspace.bind(this, workspace)}
              key={workspace.id}
            />
          );
        })}

        {this.renderDropdown()}
      </div>
    );
  }

  renderWorkspaceDeleteConfirmation() {
    if (!this.state.deleteWorkspace) {
      return null;
    }
    const ConfirmationElement = (
      <React.Fragment>
        <h5>
          {T.translate(`${PREFIX}.DeleteModal.mainMessage`, {
            workspace: this.state.deleteWorkspace.name,
          })}
        </h5>
        <div>{T.translate(`${PREFIX}.DeleteModal.helperMessage`)}</div>
      </React.Fragment>
    );

    return (
      <ConfirmationModal
        confirmationElem={ConfirmationElement}
        confirmButtonText={T.translate(`${PREFIX}.DeleteModal.confirmButton`)}
        cancelButtonText={T.translate(`${PREFIX}.DeleteModal.cancelButton`)}
        confirmFn={this.handleDeleteWorkspace.bind(this, this.state.deleteWorkspace.id)}
        cancelFn={this.toggleDeleteWorkspace.bind(this, null)}
        isOpen={true}
        headerTitle={T.translate(`${PREFIX}.DeleteModal.header`)}
        closeable={true}
        toggleModal={this.toggleDeleteWorkspace.bind(this, null)}
      />
    );
  }

  render() {
    return (
      <div className="workspace-tabs">
        {this.renderWorkspaceTabs()}
        {this.renderWorkspaceDeleteConfirmation()}
      </div>
    );
  }
}

WorkspaceTabs.propTypes = {
  workspaceId: PropTypes.string,
  onWorkspaceDelete: PropTypes.func,
  sidePanelToggle: PropTypes.bool,
};
