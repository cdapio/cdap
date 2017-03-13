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
import isNil from 'lodash/isNil';

let isPollingEnabled = true;
let pollCycleInProgress = false;
let errorStateCount = 0;
const poll = () => {
  let headers = {};
  let fetchNamespace;
  let fetchServiceStatus;
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.load('CDAP_Auth_Token');
    if (!isNil(token)) {
      headers.Authorization = 'Bearer ' +  token;
    }
    fetchNamespace = fetch('/namespacestatus', { headers, credentials: 'include' });
    fetchServiceStatus = fetch('/servicestatus', { headers, credentials: 'include' });
  } else {
    fetchNamespace = fetch('/namespacestatus', { credentials: 'include' });
    fetchServiceStatus = fetch('/servicestatus', { credentials: 'include' });
  }
  Promise.all([fetchNamespace, fetchServiceStatus])
    .then(responses => {
      let [namespaceRes, serviceStatusRes] = responses;
      serviceStatusRes.json()
        .then(serviceStatuses => {
          /*
            Check for 3 things
              1. /v3/namespaces returns a non-error response
              2. /v3/system/services/status returns a non-error response
              3. All the system services have status of 'OK'
          */
          if (namespaceRes.status < 400 && serviceStatusRes.status < 400 && serviceStatuses.body.indexOf('NOTOK') === -1) {
            let loadingState = LoadingIndicatorStore.getState().loading;
            if (loadingState.status === BACKENDSTATUS.NODESERVERDOWN) {
              window.location.reload();
            }
            if (loadingState.status === BACKENDSTATUS.BACKENDDOWN) {
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
            } else {
              errorStateCount += 1;
            }
          }
          pollCycleInProgress = false;
          startPolling();
        });
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
