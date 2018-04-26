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

import React, { Component } from 'react';
import {MyReportsApi} from 'api/reports';
import {Redirect} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';

export default class ReportsAppDelete extends Component {
  state = {
    loading: false,
    error: null,
    finished: false
  };

  stopService = () => {
    console.log('stopping service');
    MyReportsApi.stopService()
      .subscribe(() => {
        this.pollStatus();
      }, (err) => {
        console.log('failed to stop service');

        this.setState({
          loading: false,
          error: err
        });
      });
  };

  pollStatus = () => {
    let servicePoll = MyReportsApi.pollServiceStatus()
      .subscribe((res) => {
        if (res.status === 'STOPPED') {
          servicePoll.unsubscribe();
          this.deleteApplication();
        }
      });
  };

  deleteApplication = () => {
    console.log('deleting application');

    MyReportsApi.deleteApp()
      .subscribe(() => {
        this.setState({
          finished: true
        });
      }, (err) => {
        console.log('failed to delete app');

        this.setState({
          loading: false,
          error: err
        });
      });
  };

  removeApp = () => {
    this.setState({ loading: true });

    this.stopService();
  };

  renderError = () => {
    if (!this.state.error) { return null; }

    return (
      <div className="error-container text-danger">
        {this.state.error}
      </div>
    );
  };

  renderFinish = () => {
    if (!this.state.finished) { return null; }

    return (
      <Redirect to={`/ns/${getCurrentNamespace()}`} />
    );
  }

  render() {
    return (
      <div className="text-xs-center">
        <h3>Are you sure you want to delete Reports app?</h3>

        <div className="button">
          <button
            className="btn btn-danger"
            onClick={this.removeApp}
            disabled={this.state.loading}
          >
            Delete
          </button>
        </div>

        {this.renderError()}
        {this.renderFinish()}
      </div>
    );
  }
}
