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
import StatusAlertMessageStore from 'components/StatusAlertMessage/StatusAlertMessageStore';
import Alert from 'components/Alert';
import T from 'i18n-react';


export default class StatusAlertMessage extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showMessage: false
    };
  }
  componentWillMount() {
    StatusAlertMessageStore.subscribe(() => {
      let showMessage = StatusAlertMessageStore.getState().view;
      this.setState({
        showMessage
      });
      if (showMessage) {
        setTimeout(() => {
          StatusAlertMessageStore.dispatch({
            type: 'VIEWUPDATE',
            payload: {
              view: false
            }
          });
        }, 3000);
      }
    });
  }
  componentWillUnmount() {
    StatusAlertMessageStore.dispatch({
      type: 'VIEWUPDATE',
      payload: {
        view: false
      }
    });
  }
  render() {
    return (
      <Alert
        showAlert={this.state.showMessage}
        message={T.translate('features.StatusAlertMessage.message')}
        type="success"
      />
    );
  }
}
