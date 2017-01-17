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
import 'whatwg-fetch';
import LoadingIndicatorStore, {BACKENDSTATUS} from 'components/LoadingIndicator/LoadingIndicatorStore';
import StatusAlertMessageStore from 'components/StatusAlertMessage/StatusAlertMessageStore';
import debounce from 'lodash/debounce';
import T from 'i18n-react';
import cookie from 'react-cookie';
let isPollingEnabled = true;
let pollCycleInProgress = false;
let errorStateCount = 0;
const poll = () => {
  let headers = {};
  let fetchPromise;
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.load('CDAP_Auth_Token');
    headers.Authorization = 'Bearer ' +  token;
    fetchPromise = fetch('/backendstatus', { headers });
  } else {
    fetchPromise = fetch('/backendstatus');
  }
  fetchPromise
    .then(response => {
      /*
        We handle,
          - 1XX Information
          - 2XX Success
          - 3XX Redirection Status codes
      */
      if (response.status <= 399) {
        let loadingState = LoadingIndicatorStore.getState().loading;
        if ([BACKENDSTATUS.NODESERVERDOWN].indexOf(loadingState.status) !== -1) {
          window.location.reload();
        }
        if ([BACKENDSTATUS.BACKENDDOWN].indexOf(loadingState.status) !== -1) {
          StatusAlertMessageStore.dispatch({
            type: 'VIEWUPDATE',
            payload: {
              view: true
            }
          });
        }
        errorStateCount = 0;
        LoadingIndicatorStore.dispatch({
          type: BACKENDSTATUS.STATUSUPDATE,
          payload: {
            status: BACKENDSTATUS.BACKENDUP
          }
        });
      } else {
        // This is to sort of avoid the flipping of hiding and showing the
        // backend down message. Just verify twice before showing that the backend is down.
        if (errorStateCount === 2) {
          LoadingIndicatorStore.dispatch({
            type: BACKENDSTATUS.STATUSUPDATE,
            payload: {
              status: BACKENDSTATUS.BACKENDDOWN,
              message: T.translate('features.LoadingIndicator.backendDown'),
              subtitle: T.translate('features.LoadingIndicator.backendDownSubtitle')
            }
          });
        }
        errorStateCount += 1;
      }
      pollCycleInProgress = false;
      startPolling();
    })
    .catch(() => {
      LoadingIndicatorStore.dispatch({
        type: BACKENDSTATUS.STATUSUPDATE,
        payload: {
          status: BACKENDSTATUS.NODESERVERDOWN,
          message: T.translate('features.LoadingIndicator.nodeserverDown'),
          subtitle: T.translate('features.LoadingIndicator.backendDownSubtitle')
        }
      });
      pollCycleInProgress = false;
      startPolling();
    });
};
const startPolling = () => {
  if (isPollingEnabled && !pollCycleInProgress) {
    pollCycleInProgress = true;
    debounce(poll, 2000)();
  }
};

const stopPolling = () => {
  isPollingEnabled = false;
  pollCycleInProgress = false;
};

export default {
  startPolling,
  stopPolling
};
