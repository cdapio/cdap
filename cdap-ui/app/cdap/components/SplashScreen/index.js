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

import React, { Component, PropTypes } from 'react';
import 'whatwg-fetch';
import CaskVideo from 'components/CaskVideo';
require('./SplashScreen.less');

import Card from '../Card';
import MyUserStoreApi from '../../api/userstore';
import T from 'i18n-react';

 class SplashScreen extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.state = {
      error: '',
      showRegistration: window.CDAP_CONFIG.cdap.standaloneWebsiteSDKDownload,
      showSplashScreen: false,
      registrationOpen: false,
      videoOpen: false,
      first: '',
      last: '',
      email: ''
    };

    this.doNotShowCheck;
    this.toggleVideo = this.toggleVideo.bind(this);
    this.toggleRegistration = this.toggleRegistration.bind(this);
    this.toggleCheckbox = this.toggleCheckbox.bind(this);
    this.firstOnChange = this.firstOnChange.bind(this);
    this.lastOnChange = this.lastOnChange.bind(this);
    this.emailOnChange = this.emailOnChange.bind(this);
  }

  componentWillMount() {
    MyUserStoreApi.get().subscribe((res) => {
      setTimeout(() => {
        this.setState({
          showSplashScreen : window.CDAP_CONFIG.cdap.standaloneWebsiteSDKDownload && !window.CDAP_CONFIG.isEnterprise && !res.property["user-choice-hide-welcome-message"]
        });
      }, 1000);
    });
  }

  //Handles the logic of whether or not to continue showing the splash screen to this user
  resetWelcomeMessage() {
    MyUserStoreApi
      .get()
      .flatMap(res => {
        res.property['user-choice-hide-welcome-message'] = this.doNotShowCheck;
        return MyUserStoreApi.set({}, res.property);
      })
      .subscribe(
        () => {},
        (err) => { this.setState({error: err}); }
      );
  }
  toggleRegistration(){
    this.setState({
      registrationOpen : !this.state.registrationOpen
    });
  }
  onClose() {
    this.setState({
      showSplashScreen: false
    });
    this.resetWelcomeMessage();
  }
  toggleVideo(){
    this.setState({
      videoOpen : !this.state.videoOpen
    });
  }
  closeVideo(){
    if(this.state.videoOpen){
      this.setState({
        videoOpen: false
      });
    }
  }
  toggleCheckbox() {
    this.doNotShowCheck = !this.doNotShowCheck;
  }
  firstOnChange(e) {
    this.setState({first : e.target.value});
  }
  lastOnChange(e) {
    this.setState({last : e.target.value});
  }
  emailOnChange(e) {
    this.setState({email : e.target.value});
  }
  render() {

    let cardTitle = this.state.videoOpen ? '' : T.translate('features.SplashScreen.title');
    return (
      <div className={!this.state.showSplashScreen ? 'hide' : ''}>
        <div className="splash-screen-backdrop"></div>
        <div className="splash-screen">
          <Card
            className="splash-screen-card"
            closeable
            title={cardTitle}
            onClose={this.onClose.bind(this)}
          >
            <div className="text-center">
            <div className="splash-main-container">
            {
              this.state.videoOpen ?
                <div className="cask-video-container">
                  <CaskVideo />
                </div>
              :
                <div>
                  <span className="fa fa-5x icon-fist"></span>
                  <div className="version-label">
                    {T.translate('features.SplashScreen.version-label')}
                  </div>
                  <h4>
                    {T.translate('features.SplashScreen.intro-message')}
                  </h4>
                </div>
            }
            </div>

              <br />
              <div className={this.state.showRegistration ? 'group' : 'group no-registration'}>
                <a className="spash-screen-btn" target="_blank" href="http://docs.cask.co/cdap">
                  <div className="btn btn-default">
                    <span className="fa fa-book btn-icon"></span>{T.translate('features.SplashScreen.buttons.getStarted')}
                  </div>
                </a>
                <div
                  className={this.state.showRegistration ? 'btn btn-default spash-screen-btn' : 'hide'}
                  onClick={this.toggleVideo}
                >
                  <span className="fa fa-youtube-play btn-icon"></span>{T.translate('features.SplashScreen.buttons.introduction')}
                </div>
                <div
                  className={this.state.showRegistration ? 'btn btn-default spash-screen-btn' : 'hide'}
                  onClick={this.toggleRegistration}
                >
                  <span className="fa fa-pencil-square btn-icon"></span>{"Registration"}
                </div>
              </div>
              {
                this.state.showRegistration && this.state.registrationOpen ?
                  <div>
                    <div className="registration-form">
                      <div>
                          {T.translate('features.SplashScreen.registration-zero')}
                          <input onChange={this.firstOnChange} autoFocus className="first-name" type="text" name="first" id="first" placeholder={T.translate('features.SplashScreen.first-placeholder')} />
                          <input onChange={this.lastOnChange} className="last-name" type="text" name="last" id="last" placeholder={T.translate('features.SplashScreen.last-placeholder')} />
                          {T.translate('features.SplashScreen.registration-one')}
                          <div className="second-line-form">
                            {T.translate('features.SplashScreen.registration-two')}
                            <input onChange={this.emailOnChange} className="email" type="email" name="email" id="email" placeholder={T.translate('features.SplashScreen.email-placeholder')} />
                          </div>
                      </div>
                    </div>
                  </div>
                :
                  null
              }
              <div className="splash-checkbox">
                <input onChange={this.toggleCheckbox} type="checkbox" />
                <span className="splash-checkbox-label"> {T.translate('features.SplashScreen.dontShow')} </span>
              </div>
            </div>
          </Card>
        </div>
      </div>
    );
  }
}

const propTypes = {
  openVideo: PropTypes.func
};

SplashScreen.propTypes = propTypes;
export default SplashScreen;
