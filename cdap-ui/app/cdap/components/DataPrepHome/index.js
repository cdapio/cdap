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
import DataPrep from 'components/DataPrep';
import Helmet from 'react-helmet';
import T from 'i18n-react';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import {Redirect} from 'react-router-dom';
import orderBy from 'lodash/orderBy';
import DataPrepServiceControl from 'components/DataPrep/DataPrepServiceControl';
import LoadingSVG from 'components/LoadingSVG';
import DataPrepConnections from 'components/DataPrepConnections';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';

require('./DataPrepHome.scss');
/**
 *  Routing container for DataPrep for React
 **/
export default class DataPrepHome extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isEmpty: false,
      rerouteTo: null,
      error: null,
      backendDown: false,
      backendCheck: true,
      toggleConnectionsViewFlag: isNil(this.props.workspaceId) && this.props.singleWorkspaceMode ? true : false,
      currentWorkspaceId: objectQuery(this.props, 'match', 'params', 'workspaceId') || this.props.workspaceId || ''
    };

    this.namespace = NamespaceStore.getState().selectedNamespace;
    this.onServiceStart = this.onServiceStart.bind(this);
    this.toggleConnectionsView = this.toggleConnectionsView.bind(this);
    this.onWorkspaceCreate = this.onWorkspaceCreate.bind(this);
    this.updateWorkspaceList = this.updateWorkspaceList.bind(this);
    this.fetching = false;
  }

  componentWillMount() {
    this.checkBackendUp();
    this.checkWorkspaceId(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      rerouteTo: null,
      isEmpty: false,
    });
    this.checkWorkspaceId(nextProps);
  }

  checkBackendUp() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.ping({ namespace })
      .subscribe(() => {
        this.setState({
          backendCheck: false,
          backendDown: false
        });
      }, (err) => {
        if (err.statusCode === 503) {
          console.log('backend not started');

          this.setState({
            backendCheck: false,
            backendDown: true
          });

          return;
        }

        this.setState({
          backendCheck: false,
          error: err.message || err.response.message
        });
      });
  }

  toggleConnectionsView() {
    this.setState({
      toggleConnectionsViewFlag: !this.state.toggleConnectionsViewFlag
    });
  }

  onWorkspaceCreate(workspaceId) {
    this.setState({
      currentWorkspaceId: workspaceId,
      toggleConnectionsViewFlag: !this.state.toggleConnectionsViewFlag
    });
  }

  updateWorkspaceList() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.getWorkspaceList({ namespace })
      .subscribe((res) => {
        if (res.values.length === 0) {
          this.setState({
            isEmpty: true,
            toggleConnectionsViewFlag: true,
            backendDown: false,
            backendCheck: false
          });
          return;
        }
        let sortedWorkspace = orderBy(res.values, [(workspace) => workspace.name.toLowerCase()], ['asc']);
        let isCurrentWorkspaceIdValid = sortedWorkspace.find(ws => ws.id === this.props.match.params.workspaceId);
        if (this.props.match.params.workspaceId && !isCurrentWorkspaceIdValid) {
          let url = this.props.match.url.slice(0, this.props.match.url.indexOf(this.props.match.params.workspaceId));
          this.props.history.replace(url);
        }
        this.setState({
          rerouteTo: sortedWorkspace[0].id,
          backendDown: false,
          backendCheck: false,
          currentWorkspaceId: sortedWorkspace[0].id
        });

        this.fetching = false;
      });
  }

  checkWorkspaceId(props) {
    if (this.fetching || !props.match) { return; }

    this.fetching = true;

    if (!props.match.params.workspaceId) {
      this.updateWorkspaceList();
      return;
    }

    this.setState({
      isEmpty: false,
      rerouteTo: null,
      backendDown: false,
      error: null,
      currentWorkspaceId: props.match.params.workspaceId
    });

    this.fetching = false;
  }

  onServiceStart() {
    this.setState({
      backendCheck: false,
      backendDown: false
    });
    this.fetching = false;
    this.checkWorkspaceId(this.props);
  }

  renderContents() {
    let workspaceId = this.state.currentWorkspaceId;
    let {enableRouting = false, ...attributes} = this.props;
    return (
      <div className="dataprephome-wrapper">
        {
          this.state.toggleConnectionsViewFlag ?
            <DataPrepConnections
              enableRouting={enableRouting}
              onWorkspaceCreate={enableRouting ? null : this.onWorkspaceCreate}
              singleWorkspaceMode={this.props.singleWorkspaceMode}
              {...attributes}
            />
          :
            null
        }
        <DataPrep
          workspaceId={workspaceId}
          onConnectionsToggle={this.toggleConnectionsView}
          onWorkspaceDelete={this.props.singleWorkspaceMode ? null : this.updateWorkspaceList}
          onSubmit={this.props.onSubmit}
          singleWorkspaceMode={this.props.singleWorkspaceMode}
        />
      </div>
    );
  }

  render() {
    if (this.state.backendCheck) {
      return (
        <div className="text-xs-center">
          <LoadingSVG />
        </div>
      );
    }

    if (this.state.backendDown) {
      return (
        <DataPrepServiceControl
          onServiceStart={this.onServiceStart}
        />
      );
    }

    if (!this.props.singleWorkspaceMode && this.state.isEmpty) {
      return (
        <Redirect to={`/ns/${this.namespace}/connections/browser`} />
      );
    }

    return (
      <div>
        {
          !this.props.singleWorkspaceMode ?
            <Helmet
              title={T.translate('features.DataPrep.pageTitle')}
            />
          :
            null
        }
        {
          this.renderContents()
        }
      </div>
    );
  }
}


DataPrepHome.propTypes = {
  match: PropTypes.shape({
    params: PropTypes.shape({
      workspaceId: PropTypes.string
    }),
    url: PropTypes.string
  }),
  location: PropTypes.object,
  history: PropTypes.object,

  /*
    The following 4 are used when coming from pipeline studio
    1. 'enableRouting' is used when toggling connections side-by-side with dataprep
       - When set to 'true' treats all anchors as it is. Navigation happens on click of any anchor tag and the url
         is updated. This is for DataPrepConnections when navigating from '/connections' url
       - When set to 'false' suppresses all anchor navigations.
          The final click on a file (or eventually a database table) should navigate to dataprep via a url update
          - User goes to go /dataprep
          - Toggles connections arrow to choose another file
          - Navigates through differnet connection and folders
          - Chooses a file which then navigates to a workspaceId (using url)
    2. 'singleWorkspaceMode`
       - When set to 'true' will suppress all anchor navigations. Everything is in-memory and needs to update based
         on store. This is used when user wrangles data from pipeline studio.
  */
  enableRouting: PropTypes.bool,
  singleWorkspaceMode: PropTypes.bool,
  workspaceId: PropTypes.string,
  onSubmit: PropTypes.func
};
