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

import React, {Component, PropTypes} from 'react';
import classnames from 'classnames';
import LoadingIndicatorStore, {BACKENDSTATUS, LOADINGSTATUS} from 'components/LoadingIndicator/LoadingIndicatorStore';
import T from 'i18n-react';
import {Modal} from 'reactstrap';

require('./LoadingIndicator.scss');

export default class LoadingIndicator extends Component {
  constructor(props) {
    super(props);
    let {message = '', subtitle = ''} = this.props;
    this.state = {
      showLoading: false,
      message,
      subtitle
    };
  }
  componentDidMount() {
    this.loadingIndicatorStoreSubscription = LoadingIndicatorStore.subscribe(() => {
      let {status, message = '', subtitle = ''} = LoadingIndicatorStore.getState().loading;
      let showLoading;
      if ([BACKENDSTATUS.BACKENDUP, BACKENDSTATUS.NODESERVERUP, LOADINGSTATUS.HIDELOADING].indexOf(status) !== -1) {
        showLoading = false;
      }
      if ([LOADINGSTATUS.SHOWLOADING, BACKENDSTATUS.NODESERVERDOWN, BACKENDSTATUS.BACKENDDOWN].indexOf(status) !== -1) {
        showLoading = true;
      }
      this.setState({
        showLoading,
        message,
        subtitle
      });
    });
  }
  componentWillUnmount() {
    this.loadingIndicatorStoreSubscription();
  }
  render() {
    return this.state.showLoading ?
      (
        <Modal
          isOpen={this.state.showLoading}
          toggle={() =>{}}
          className="loading-indicator"
        >

          <div className="text-xs-center">
            {this.props.icon ? (
              <div className={classnames('fa', this.props.icon)}></div>
            ) : (
              <div className="icon-loading-bars">
                <svg
                  className="loading-bar"
                  version="1.1"
                  x="0px"
                  y="0px"
                  width="24px"
                  height="30px"
                  viewBox="0 0 24 30"
                >
                  <rect
                    x="0"
                    y="10"
                    width="4"
                    height="10"
                    fill="#333"
                    opacity="0.2"
                  >
                    <animate
                      attributeName="opacity"
                      attributeType="XML"
                      values="0.2; 1; .2"
                      begin="0s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                    <animate
                      attributeName="height"
                      attributeType="XML"
                      values="10; 20; 10"
                      begin="0s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                    <animate
                      attributeName="y"
                      attributeType="XML"
                      values="10; 5; 10"
                      begin="0s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                  </rect>
                  <rect
                    x="8"
                    y="10"
                    width="4"
                    height="10"
                    fill="#333"
                    opacity="0.2"
                  >
                    <animate
                      attributeName="opacity"
                      attributeType="XML"
                      values="0.2; 1; .2"
                      begin="0.15s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                    <animate
                      attributeName="height"
                      attributeType="XML"
                      values="10; 20; 10"
                      begin="0.15s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                    <animate
                      attributeName="y"
                      attributeType="XML"
                      values="10; 5; 10"
                      begin="0.15s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                  </rect>
                  <rect
                    x="16"
                    y="10"
                    width="4"
                    height="10"
                    fill="#333"
                    opacity="0.2"
                  >
                    <animate
                      attributeName="opacity"
                      attributeType="XML"
                      values="0.2; 1; .2"
                      begin="0.3s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                    <animate
                      attributeName="height"
                      attributeType="XML"
                      values="10; 20; 10"
                      begin="0.3s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                    <animate
                      attributeName="y"
                      attributeType="XML"
                      values="10; 5; 10"
                      begin="0.3s"
                      dur="0.6s"
                      repeatCount="indefinite"
                    />
                  </rect>
                </svg>
              </div>
            )}
            <h2> {this.state.message} </h2>
            <h4>{this.state.subtitle}</h4>
          </div>
        </Modal>
      )
    :
      null;
  }
}
LoadingIndicator.defaultProps = {
  icon: '',
  message: T.translate('features.LoadingIndicator.defaultMessage')
};

LoadingIndicator.propTypes = {
  message: PropTypes.string,
  subtitle: PropTypes.string,
  icon: PropTypes.string
};
