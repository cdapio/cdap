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
            <div className={classnames('fa', this.props.icon)}></div>
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
  icon: 'icon-fist',
  message: T.translate('features.LoadingIndicator.defaultMessage')
};

LoadingIndicator.propTypes = {
  message: PropTypes.string,
  subtitle: PropTypes.string,
  icon: PropTypes.string
};
