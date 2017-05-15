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
import sortBy from 'lodash/sortBy';
import DataPrepServiceControl from 'components/DataPrep/DataPrepServiceControl';
import LoadingSVG from 'components/LoadingSVG';

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
      backendCheck: true
    };

    this.namespace = NamespaceStore.getState().selectedNamespace;
    this.onServiceStart = this.onServiceStart.bind(this);

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

  checkWorkspaceId(props) {
    if (this.fetching) { return; }

    this.fetching = true;

    if (!props.match.params.workspaceId) {
      let namespace = NamespaceStore.getState().selectedNamespace;

      MyDataPrepApi.getWorkspaceList({ namespace })
        .subscribe((res) => {
          if (res.values.length === 0) {
            this.setState({
              isEmpty: true,
              backendDown: false,
              backendCheck: false
            });
            return;
          }
          let sortedWorkspace = sortBy(res.values, ['name']);

          this.setState({
            rerouteTo: sortedWorkspace[0].id,
            backendDown: false,
            backendCheck: false
          });

          this.fetching = false;
        });

      return;
    }

    this.setState({
      isEmpty: false,
      rerouteTo: null,
      backendDown: false,
      error: null
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

    if (this.state.isEmpty) {
      return (
        <Redirect to={`/ns/${this.namespace}/connections/browser`} />
      );
    }

    if (this.state.rerouteTo) {
      return (
        <Redirect to={`/ns/${this.namespace}/dataprep/${this.state.rerouteTo}`} />
      );
    }

    return (
      <div>
        <Helmet
          title={T.translate('features.DataPrep.pageTitle')}
        />
        <DataPrep
          workspaceId={this.props.match.params.workspaceId}
        />
      </div>
    );
  }
}

DataPrepHome.propTypes = {
  match: PropTypes.shape({
    params: PropTypes.shape({
      workspaceId: PropTypes.string
    })
  })
};
