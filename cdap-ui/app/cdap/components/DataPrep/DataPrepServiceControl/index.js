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

import React, { Component } from 'react';
import MyDataPrepApi from 'api/dataprep';
import {MyArtifactApi} from 'api/artifact';
import find from 'lodash/find';
import NamespaceStore from 'services/NamespaceStore';

export default class DataPrepServiceControl extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: false,
      error: null,
      extendedMessage: null
    };

    this.enableService = this.enableService.bind(this);
  }

  componentWillUnmount() {
    if (this.servicePoll && this.servicePoll.dispose) {
      this.servicePoll.dispose();
    }
  }

  enableService() {
    this.setState({loading: true});

    /**
     *  1. Get Wrangler Service App
     *  2. If not found, create app
     *  3. Start Wrangler Service
     *  4. Poll until service starts, then reload page
     **/

    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.getApp({ namespace })
      .subscribe(() => {
        // Wrangler app already exist
        // Just start service
        this.startService();
      }, () => {
        // App does not exist
        // Go to create app
        this.createApp();
      });
  }

  createApp() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyArtifactApi.list({ namespace })
      .subscribe((res) => {
        let artifact = find(res, { 'name': 'wrangler-service' });

        MyDataPrepApi.createApp({ namespace }, { artifact })
          .subscribe(() => {
            this.startService();
          }, (err) => {
            this.setState({
              error: 'Failed to enable data preparation',
              extendedMessage: err.data || err,
              loading: false
            });
          });
      });
  }

  startService() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.startService({ namespace })
      .subscribe(() => {
        this.pollServiceStatus();
      }, (err) => {
        this.setState({
          error: 'Failed to enable data preparation',
          extendedMessage: err.data || err,
          loading: false
        });
      });
  }

  pollServiceStatus() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    this.servicePoll = MyDataPrepApi.pollServiceStatus({ namespace })
      .subscribe((res) => {
        if (res.status === 'RUNNING') {
          window.location.reload();
        }
      }, (err) => {
        this.setState({
          error: 'Failed to enable data preparation',
          extendedMessage: err.data || err,
          loading: false
        });
      });
  }

  renderError() {
    if (!this.state.error) { return null; }

    return (
      <div>
        <h5 className="text-danger text-xs-center">
          {this.state.error}
        </h5>
        <p className="text-dangertext-xs-center">
          {this.state.extendedMessage}
        </p>
      </div>
    );
  }

  render() {
    return (
      <div className="wrangler-container error">
        <h4 className="text-xs-center">
          Data Preparation (Beta) is not enabled. Please enable it.
        </h4>
        <br/>
        <div className="text-xs-center">
          <button
            className="btn btn-primary"
            onClick={this.enableService}
            disabled={this.state.loading}
          >
            {
              !this.state.loading ? 'Enable Data Preparation Service' : (
                <span>
                  <span className="fa fa-spin fa-spinner" /> Enabling...
                </span>
              )
            }
          </button>
        </div>

        <br/>

        {this.renderError()}
      </div>
    );
  }
}
