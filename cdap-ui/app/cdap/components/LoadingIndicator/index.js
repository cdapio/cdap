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

import React, {Component} from 'react';
import LoadingIndicatorStore, {BACKENDSTATUS, LOADINGSTATUS} from 'components/LoadingIndicator/LoadingIndicatorStore';
import T from 'i18n-react';
import {Modal} from 'reactstrap';
import LoadingSVG from 'components/LoadingSVG';

const PREFIX = 'features.LoadingIndicator';

require('./LoadingIndicator.scss');

export default class LoadingIndicator extends Component {

  static defaultProps = {
    icon: '',
    message: T.translate('features.LoadingIndicator.defaultMessage')
  };

  state = {
    showLoading: false
  }

  componentDidMount() {
    this.loadingIndicatorStoreSubscription = LoadingIndicatorStore.subscribe(() => {
      if (location.pathname.indexOf('/cdap/administration') !== -1) {
        return;
      }
      let {status, services = []} = LoadingIndicatorStore.getState().loading;
      let showLoading;
      if ([BACKENDSTATUS.BACKENDUP, BACKENDSTATUS.NODESERVERUP, LOADINGSTATUS.HIDELOADING].indexOf(status) !== -1) {
        showLoading = false;
      }
      if ([LOADINGSTATUS.SHOWLOADING, BACKENDSTATUS.NODESERVERDOWN, BACKENDSTATUS.BACKENDDOWN].indexOf(status) !== -1) {
        showLoading = true;
      }
      if (this.state.showLoading !== showLoading) {
        this.setState({
          showLoading,
          services,
          status
        });
      }
    });
  }
  componentWillUnmount() {
    this.loadingIndicatorStoreSubscription();
  }

  renderCallsToAction() {
    return (
      <div className="subtitle">
        <strong> {T.translate(`${PREFIX}.tryMessage`)}</strong>
        {
          this.state.status === BACKENDSTATUS.NODESERVERDOWN ?
            null
          :
            <a href="/cdap/administration"> {T.translate(`${PREFIX}.systemDashboard`)}</a>
        }
        <span> {T.translate(`${PREFIX}.contactadmin`)} </span>
      </div>
    );
  }
  renderContent() {
    let message;
    let {loading} = LoadingIndicatorStore.getState();
    if (loading.status === BACKENDSTATUS.BACKENDDOWN) {
      if (this.state.services.length === 1) {
        message = T.translate(`${PREFIX}.serviceDown`, {serviceName: this.state.servives[0].name});
      } else {
        message = T.translate(`${PREFIX}.servicesDown`);
      }
    }
    if (loading.status === BACKENDSTATUS.NODESERVERDOWN) {
      message = T.translate(`${PREFIX}.nodeserverDown`);
    }

    return (
      <div>
        <h2> {message} </h2>
        {this.renderCallsToAction()}
      </div>
    );
  }
  render() {
    if (!this.state.showLoading) {
      return null;
    }
    return (
      <Modal
        isOpen={this.state.showLoading}
        toggle={() =>{}}
        zIndex={2000}
        className="loading-indicator"
      >

        <div className="text-xs-center">
          <div className="icon-loading-bars">
            <LoadingSVG />
          </div>
          {this.renderContent()}
        </div>
      </Modal>
    );
  }
}
