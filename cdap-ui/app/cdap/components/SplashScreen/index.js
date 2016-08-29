/*
 * Copyright © 2016 Cask Data, Inc.
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
import 'whatwg-fetch';
require('./SplashScreen.less');

import Card from '../Card';

export default class SplashScreen extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.state = {
      error: '',
      showRegistration: window.CDAP_CONFIG.cdap.standaloneWebsiteSDKDownload === 'true',
      showSplashScreen: window.CDAP_CONFIG.cdap.showStandaloneWelcomeMessage === 'true'
    };
  }
  resetWelcomeMessage() {
    fetch('/resetWelcomeMessage', {
      method: 'POST',
      headers: {
        'Accept': 'text/javascript'
      }
    })
      .then((response) => {
        if (response.status > 300) {
          this.setState({error: response.statusText});
        }
      });
  }
  onClose() {
    this.setState({
      showSplashScreen: false
    });
    this.resetWelcomeMessage();
  }
  render() {
    return (
      <div className={!this.state.showSplashScreen ? 'hide' : ''}>
        <div className="splash-screen-backdrop"></div>
        <div className="splash-screen">
          <Card
            title="Welcome to Cask Data Application Platform"
            error={this.state.error}
            success={this.state.message}
            closeable
            onClose={this.onClose.bind(this)}
          >
            <div className="text-center">
              <span className="fa fa-5x icon-fist"></span>
              <h4>
                Your platform for building exciting analytical data applications
              </h4>
              <br />
              <div className={this.state.showRegistration ? 'group' : 'group no-registration'}>
                <div className="btn btn-default">
                  Get Started
                </div>
                <div className={this.state.showRegistration ? 'btn btn-default' : 'hide'}>
                  Introduction to CDAP
                </div>
                <div className={this.state.showRegistration ? 'btn btn-default' : 'hide'}>
                  Register for Updates
                </div>
              </div>
            </div>
          </Card>
        </div>
      </div>
    );
  }
}
