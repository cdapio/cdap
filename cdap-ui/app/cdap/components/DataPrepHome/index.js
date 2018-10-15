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
import DataPrep, { MIN_DATAPREP_VERSION } from 'components/DataPrep';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import Helmet from 'react-helmet';
import T from 'i18n-react';
import MyDataPrepApi from 'api/dataprep';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { Redirect } from 'react-router-dom';
import orderBy from 'lodash/orderBy';
import DataPrepServiceControl from 'components/DataPrep/DataPrepServiceControl';
import LoadingSVG from 'components/LoadingSVG';
import DataPrepConnections from 'components/DataPrepConnections';
import { objectQuery } from 'services/helpers';
import isNil from 'lodash/isNil';
import ee from 'event-emitter';
import Version from 'services/VersionRange/Version';
import { Theme } from 'services/ThemeHelper';
import { setWorkspace } from 'components/DataPrep/store/DataPrepActionCreator';

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
      isMinVersionMet: false,
      toggleConnectionsViewFlag:
        isNil(this.props.workspaceId) && this.props.singleWorkspaceMode ? true : false,
      currentWorkspaceId:
        objectQuery(this.props, 'match', 'params', 'workspaceId') || this.props.workspaceId || '',
    };

    this.namespace = getCurrentNamespace();
    this.onServiceStart = this.onServiceStart.bind(this);
    this.toggleConnectionsView = this.toggleConnectionsView.bind(this);
    this.onWorkspaceCreate = this.onWorkspaceCreate.bind(this);
    this.updateWorkspaceList = this.updateWorkspaceList.bind(this);
    this.eventEmitter = ee(ee);
    this.fetching = false;
  }

  componentWillMount() {
    this.checkBackendUp();
  }

  componentWillReceiveProps(nextProps) {
    if (this.state.backendCheck) {
      return;
    }

    this.setState({
      rerouteTo: null,
      isEmpty: false,
    });
    this.checkWorkspaceId(nextProps);
  }

  checkBackendUp() {
    let namespace = getCurrentNamespace();
    MyDataPrepApi.ping({ namespace })
      .combineLatest(MyDataPrepApi.getApp({ namespace }))
      .subscribe(
        (res) => {
          const appSpec = res[1];

          let minimumVersion = new Version(MIN_DATAPREP_VERSION);

          if (minimumVersion.compareTo(new Version(appSpec.artifactVersion)) > 0) {
            console.log('dataprep minimum version not met');

            this.setState({
              backendCheck: false,
              backendDown: true,
            });

            return;
          }

          this.setState({
            isMinVersionMet: true,
            backendCheck: false,
          });

          this.checkWorkspaceId(this.props);
        },
        (err) => {
          if (err.statusCode === 503) {
            console.log('backend not started');

            this.setState({
              backendCheck: false,
              backendDown: true,
            });

            return;
          }

          this.setState({
            backendCheck: false,
            error: true,
          });
        }
      );
  }

  toggleConnectionsView() {
    this.setState({
      toggleConnectionsViewFlag: !this.state.toggleConnectionsViewFlag,
    });
  }

  onWorkspaceCreate(workspaceId) {
    this.setState({
      currentWorkspaceId: workspaceId,
      toggleConnectionsViewFlag: !this.state.toggleConnectionsViewFlag,
    });

    this.eventEmitter.emit('DATAPREP_CLOSE_SIDEPANEL');
  }

  updateWorkspaceListRetry(namespace) {
    MyDataPrepApi.getWorkspaceList({ namespace }).subscribe(
      (res) => {
        if (res.values.length === 0) {
          this.setState({
            isEmpty: true,
            toggleConnectionsViewFlag: true,
            backendDown: false,
            backendCheck: false,
          });
          DataPrepStore.dispatch({
            type: DataPrepActions.disableLoading,
          });
          DataPrepStore.dispatch({
            type: DataPrepActions.setWorkspaceList,
            payload: {
              list: [],
            },
          });

          return;
        }
        let sortedWorkspace = orderBy(
          res.values,
          [(workspace) => workspace.name.toLowerCase()],
          ['asc']
        );
        DataPrepStore.dispatch({
          type: DataPrepActions.setWorkspaceList,
          payload: {
            list: sortedWorkspace,
          },
        });

        let isCurrentWorkspaceIdValid = sortedWorkspace.find(
          (ws) => ws.id === this.props.match.params.workspaceId
        );
        if (this.props.match.params.workspaceId && !isCurrentWorkspaceIdValid) {
          let url = this.props.match.url.slice(
            0,
            this.props.match.url.indexOf(this.props.match.params.workspaceId)
          );
          this.props.history.replace(url);
        } else {
          setWorkspace(sortedWorkspace[0].id).subscribe();
        }
        this.setState({
          rerouteTo: sortedWorkspace[0].id,
          backendDown: false,
          backendCheck: false,
          currentWorkspaceId: sortedWorkspace[0].id,
        });
        DataPrepStore.dispatch({
          type: DataPrepActions.disableLoading,
        });

        this.fetching = false;
      },
      (err) => {
        if (err.statusCode === 503) {
          return;
        }

        if (this.workspaceListRetries < 3) {
          this.workspaceListRetries += 1;
          this.updateWorkspaceListRetry(namespace);
        } else {
          this.setState({
            backendDown: false,
            backendCheck: false,
          });
          DataPrepStore.dispatch({
            type: DataPrepActions.disableLoading,
          });
          DataPrepStore.dispatch({
            type: DataPrepActions.setDataError,
            payload: {
              errorMessage: true,
            },
          });
        }
      }
    );
  }

  updateWorkspaceList() {
    let namespace = getCurrentNamespace();

    this.workspaceListRetries = 0;
    DataPrepStore.dispatch({
      type: DataPrepActions.reset,
    });
    this.updateWorkspaceListRetry(namespace);
  }

  checkWorkspaceId(props) {
    if (this.fetching || !props.match) {
      return;
    }

    this.fetching = true;

    if (!props.match.params.workspaceId) {
      this.updateWorkspaceList();
      return;
    }

    this.setState({
      isEmpty: false,
      rerouteTo: null,
      backendDown: false,
      backendCheck: false,
      error: null,
      currentWorkspaceId: props.match.params.workspaceId,
    });

    this.fetching = false;
  }

  onServiceStart() {
    this.setState({
      backendCheck: false,
      backendDown: false,
      isMinVersionMet: true,
    });
    this.fetching = false;
    this.checkWorkspaceId(this.props);
  }

  renderContents() {
    let workspaceId = this.state.currentWorkspaceId;
    let { enableRouting = false, ...attributes } = this.props;
    return (
      <div className="dataprephome-wrapper">
        {this.state.toggleConnectionsViewFlag ? (
          <DataPrepConnections
            enableRouting={enableRouting}
            onWorkspaceCreate={enableRouting ? null : this.onWorkspaceCreate}
            singleWorkspaceMode={this.props.singleWorkspaceMode}
            {...attributes}
          />
        ) : null}
        {!workspaceId && this.props.singleWorkspaceMode ? null : (
          <DataPrep
            workspaceId={workspaceId}
            onConnectionsToggle={this.toggleConnectionsView}
            onWorkspaceDelete={this.props.singleWorkspaceMode ? null : this.updateWorkspaceList}
            onSubmit={this.props.onSubmit}
            singleWorkspaceMode={this.props.singleWorkspaceMode}
          />
        )}
      </div>
    );
  }

  render() {
    let pageTitle = (
      <Helmet
        title={T.translate('features.DataPrep.pageTitle', {
          productName: Theme.productName,
        })}
      />
    );
    const renderPageTitle = () => {
      return !this.props.singleWorkspaceMode ? pageTitle : null;
    };

    if (this.state.backendCheck) {
      return (
        <div className="text-center">
          {renderPageTitle()}
          <LoadingSVG />
        </div>
      );
    }

    if (this.state.backendDown || !this.state.isMinVersionMet) {
      return (
        <div>
          {renderPageTitle()}
          <DataPrepServiceControl onServiceStart={this.onServiceStart} />
        </div>
      );
    }

    if (!this.props.singleWorkspaceMode && this.state.isEmpty) {
      return <Redirect to={`/ns/${this.namespace}/connections`} />;
    }

    return (
      <div>
        {renderPageTitle()}
        {this.renderContents()}
      </div>
    );
  }
}

DataPrepHome.propTypes = {
  match: PropTypes.shape({
    params: PropTypes.shape({
      workspaceId: PropTypes.string,
    }),
    url: PropTypes.string,
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
  onSubmit: PropTypes.func,
};
