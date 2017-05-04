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
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import {Link, Redirect} from 'react-router-dom';
import {setWorkspace, getWorkspaceList} from 'components/DataPrep/store/DataPrepActionCreator';
import IconSVG from 'components/IconSVG';
import cookie from 'react-cookie';

require('./WorkspaceTabs.scss');

const MAX_NUM_TABS = 7;

export default class WorkspaceTabs extends Component {
  constructor(props) {
    super(props);

    let initialState = DataPrepStore.getState();

    this.state = {
      activeWorkspace: initialState.dataprep.workspaceId,
      workspaceList: initialState.workspaces.list,
      beginIndex: 0,
      isEmpty: false,
      reroute: false
    };

    this.namespace = NamespaceStore.getState().selectedNamespace;

    this.getWorkspaceList = this.getWorkspaceList.bind(this);
    this.next = this.next.bind(this);
    this.prev = this.prev.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState();

      this.setState({
        activeWorkspace: state.dataprep.workspaceId,
        workspaceList: state.workspaces.list
      });
    });
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
  }

  getWorkspaceList() {
    getWorkspaceList();
  }

  setActiveWorkspace(workspace) {
    DataPrepStore.dispatch({
      type: DataPrepActions.enableLoading
    });

    setWorkspace(workspace.id)
      .subscribe(() => {
        cookie.save('DATAPREP_WORKSPACE', workspace.id, { path: '/' });
        DataPrepStore.dispatch({
          type: DataPrepActions.disableLoading
        });
      }, (err) => {
        console.log('err', err);
        DataPrepStore.dispatch({
          type: DataPrepActions.disableLoading
        });
      });
    }

  deleteWorkspace(workspaceId) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.delete({
      namespace,
      workspaceId
    }).subscribe(() => {
      if (workspaceId === this.state.activeWorkspace) {
        this.setState({reroute: true});
        return;
      }
      this.getWorkspaceList();

    }, (err) => {
      console.log("Error Deleting", err);
    });
  }

  next() {
    if (this.state.beginIndex + MAX_NUM_TABS >= this.state.workspaceList.length) { return; }

    this.setState({
      beginIndex: this.state.beginIndex + 1
    });
  }

  prev() {
    if (this.state.beginIndex <= 0) { return; }

    this.setState({
      beginIndex: this.state.beginIndex - 1
    });
  }

  renderActiveWorkspace(workspace) {
    return (
      <div
        key={workspace.id}
        className="workspace-tab active"
      >
        <span title={workspace.name}>
          {workspace.name}
        </span>

        <span
          className="fa fa-fw delete-workspace"
          onClick={this.deleteWorkspace.bind(this, workspace.id)}
        >
          <IconSVG
            name="icon-close"
          />
        </span>
      </div>
    );
  }

  renderInactiveWorkspace(workspace) {
    return (
      <div
        key={workspace.id}
        className="workspace-tab"
      >
        <Link
          to={`/ns/${this.namespace}/dataprep/${workspace.id}`}
          title={workspace.name}
        >
          {workspace.name}
        </Link>

        <span
          className="fa fa-fw delete-workspace"
          onClick={this.deleteWorkspace.bind(this, workspace.id)}
        >
          <IconSVG
            name="icon-close"
          />
        </span>
      </div>
    );

  }

  renderWorkspaceTabs() {
    let beginIndex = this.state.beginIndex;
    let endIndex = beginIndex + MAX_NUM_TABS;

    let displayWorkspace = this.state.workspaceList.slice(beginIndex, endIndex);

    let prevArrow;
    if (this.state.beginIndex !== 0) {
      prevArrow = (
        <span
          className="tab-arrow text-xs-center"
          onClick={this.prev}
        >
          <span className="fa fa-chevron-left" />
        </span>
      );
    }

    let nextArrow;
    if (this.state.beginIndex + MAX_NUM_TABS < this.state.workspaceList.length) {
      nextArrow = (
        <span
          className="tab-arrow text-xs-center"
          onClick={this.next}
        >
          <span className="fa fa-chevron-right" />
        </span>
      );
    }

    return (
      <div className="workspace-tabs-list">
        {prevArrow}

        {
          displayWorkspace.map((workspace) => {
            return this.state.activeWorkspace === workspace.id ?
              this.renderActiveWorkspace(workspace)
            :
              this.renderInactiveWorkspace(workspace);
          })
        }

        {nextArrow}
      </div>
    );
  }

  render() {
    if (this.state.isEmpty) {
      return (
        <Redirect to={`/ns/${this.namespace}/connections/browser`} />
      );
    }

    if (this.state.reroute) {
      return (
        <Redirect to={`/ns/${this.namespace}/dataprep`} />
      );
    }

    if (this.props.singleWorkspaceMode) {
      return (
        <div>
          {this.renderActiveWorkspace(this.state.activeWorkspace)}
        </div>
      );
    }
    return (
      <div className="workspace-tabs">
        {this.renderWorkspaceTabs()}
      </div>
    );
  }
}

WorkspaceTabs.propTypes = {
  singleWorkspaceMode: PropTypes.bool,
  workspaceId: PropTypes.string
};
