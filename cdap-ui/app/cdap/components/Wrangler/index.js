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
import WranglerTopPanel from 'components/Wrangler/TopPanel';
import WranglerTable from 'components/Wrangler/WranglerTable';
import WranglerSidePanel from 'components/Wrangler/WranglerSidePanel';
import WranglerCLI from 'components/Wrangler/WranglerCLI';
import MyWranglerApi from 'api/wrangler';
import cookie from 'react-cookie';
import WranglerStore from 'components/Wrangler/store';
import WranglerActions from 'components/Wrangler/store/WranglerActions';
import WranglerServiceControl from 'components/Wrangler/WranglerServiceControl';
import ee from 'event-emitter';

require('./Wrangler.scss');

export default class Wrangler extends Component {
  constructor(props) {
    super(props);

    this.state = {
      backendDown: false
    };

    this.toggleBackendDown = this.toggleBackendDown.bind(this);
    this.eventEmitter = ee(ee);

    this.eventEmitter.on('WRANGLER_BACKEND_DOWN', this.toggleBackendDown);
  }

  componentWillMount() {
    let workspaceId = cookie.load('WRANGLER_WORKSPACE');
    let params = {
      namespace: 'default',
      workspaceId: workspaceId,
      limit: 100
    };

    MyWranglerApi.execute(params)
      .subscribe((res) => {
        WranglerStore.dispatch({
          type: WranglerActions.setWorkspace,
          payload: {
            data: res.value,
            headers: res.header,
            workspaceId
          }
        });
      }, (err) => {
        if (err.statusCode === 503) {
          console.log('backend not started');
          this.eventEmitter.emit('WRANGLER_BACKEND_DOWN');
          return;
        }

        console.log('Init Error', err);
        cookie.remove('WRANGLER_WORKSPACE');

        WranglerStore.dispatch({
          type: WranglerActions.setInitialized
        });
        this.eventEmitter.emit('WRANGLER_NO_WORKSPACE_ID');
      });
  }

  componentWillUnmount() {
    WranglerStore.dispatch({
      type: WranglerActions.reset
    });
    this.eventEmitter.off('WRANGLER_BACKEND_DOWN', this.toggleBackendDown);
  }

  toggleBackendDown() {
    this.setState({backendDown: true});
  }

  renderBackendDown() {
    if (!this.state.backendDown) { return null; }

    return (
      <WranglerServiceControl
        onEnable={this.toggleBackendDown}
      />
    );
  }

  render() {
    if (this.state.backendDown) { return this.renderBackendDown(); }

    return (
      <div className="wrangler-container">
        <WranglerTopPanel />

        <div className="row wrangler-body">
          <div className="wrangler-main col-xs-9">
            <WranglerTable />
            <WranglerCLI />
          </div>

          <WranglerSidePanel />
        </div>
      </div>
    );
  }
}
