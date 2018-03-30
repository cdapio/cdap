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
import {MyProfileApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
require('./ListView.scss');

export default class ProfilesListView extends Component {
  state = {
    profiles: [],
    error: null,
    loading: true
  };

  componentDidMount() {
    MyProfileApi.list({
      namespace: getCurrentNamespace()
    })
    .subscribe(
      profiles => {
        this.setState({
          profiles,
          loading: false
        });
      },
      err => {
        this.setState({
          error: err,
          loading: false
        });
      }
    );
  }
  render() {
    if (this.state.error) {
      return (
        <div className="text-danger">
          {JSON.stringify(this.state.error, null, 2)}
        </div>
      );
    }
    return (
      <div className="profiles-list-view">
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-item sub-header">
              <div />
              <div />
              <div />
              <div />
              <div />
              <div />
              <div className="sub-title">Pipeline Usage</div>
              <div/>
              <div className="sub-title">Associations</div>
              <div/>
              <div/>
              <div/>
            </div>
            <div className="grid-item">
              <div></div>
              <strong> Profile Name </strong>
              <strong> Provider </strong>
              <strong> Scope </strong>
              <strong> Pipelines </strong>
              <strong> Last 24hrs runs </strong>
              <strong> Last 24hrs <br /> node/hr </strong>
              <strong> Total <br /> node/hr </strong>
              <strong> Schedules </strong>
              <strong> Triggers </strong>
              <div></div>
              <div></div>
            </div>
          </div>
          <div className="grid-body">
            {
              this.state.profiles.map(profile => {
                return (
                  <div className="grid-item">
                    <div></div>
                    <div>{profile.name}</div>
                    <div>{profile.provisioner.name}</div>
                    <div>{profile.scope}</div>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  }
}
