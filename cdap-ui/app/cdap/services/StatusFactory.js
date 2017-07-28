/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import 'whatwg-fetch';
import LoadingIndicatorStore, {
  BACKENDSTATUS
} from 'components/LoadingIndicator/LoadingIndicatorStore';
import StatusAlertMessageStore from 'components/StatusAlertMessage/StatusAlertMessageStore';
import cookie from 'react-cookie';
import isNil from 'lodash/isNil';
import Rx from 'rx';
import SystemServicesStore, {pollSystemServices} from 'services/SystemServicesStore';

let pollingObservable;
let systemServiceSubscription;
let retries = 0;

const parseAndDispatchBackendStatus = response => {
  /*
    We handle,
      - 1XX Information
      - 2XX Success
      - 3XX Redirection Status codes
  */
  if (retries < 3) {
    retries += 1;
    return;
  }
  let loadingState = LoadingIndicatorStore.getState().loading;
  if (response.status <= 399) {
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
      LoadingIndicatorStore.dispatch({
        type: BACKENDSTATUS.STATUSUPDATE,
        payload: {
          status: BACKENDSTATUS.BACKENDUP
        }
      });
      retries = 0;
    }
  }
  if (response.status === 404 && loadingState.status !== BACKENDSTATUS.NODESERVERDOWN) {
    dispatchNodeServerDown();
  }
  /*
    TODO: Need to handle 401 and 403.
  */
  if (response.status > 404) {
    LoadingIndicatorStore.dispatch({
      type: BACKENDSTATUS.STATUSUPDATE,
      payload: {
        status: BACKENDSTATUS.BACKENDDOWN
      }
    });
  }
};

const dispatchNodeServerDown = () => {
  LoadingIndicatorStore.dispatch({
    type: BACKENDSTATUS.STATUSUPDATE,
    payload: {
      status: BACKENDSTATUS.NODESERVERDOWN
    }
  });
};

const getRequestInfo = () => {
  let headers = {};
  let requestInfo = {
    credentials: 'include'
  };
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.load('CDAP_Auth_Token');
    if (!isNil(token)) {
      headers.Authorization = 'Bearer ' + token;
    }
    requestInfo.headers = headers;
  }
  return requestInfo;
};

const startServicePolling = () => {
  systemServiceSubscription = SystemServicesStore
    .subscribe(
      () => {
        let {services} = SystemServicesStore.getState();
        services = services.filter(service => service.status === 'NOTOK');
        if (services.length) {
          LoadingIndicatorStore.dispatch({
            type: BACKENDSTATUS.STATUSUPDATE,
            payload: {
              services
            }
          });
        }
      }
    );
};

const startPolling = () => {
  pollSystemServices();
  stopPolling();
  startServicePolling();
  pollingObservable = Rx.Observable
    .interval(2000)
    .flatMap(() =>
      Rx.Observable
        .fromPromise(fetch('/backendstatus', getRequestInfo()))
        .catch(error => {
          dispatchNodeServerDown();
          return Rx.Observable.of(`Error: ${error}`);
        })
    )
    .subscribe(parseAndDispatchBackendStatus, dispatchNodeServerDown);
};

const stopServicePolling = () => {
  if (systemServiceSubscription) {
    systemServiceSubscription();
    systemServiceSubscription = null;
  }
};

const stopPolling = () => {
  stopServicePolling();
  if (pollingObservable) {
    pollingObservable.dispose();
  }
};

export default {
  startPollingForBackendStatus: startPolling,
  stopPollingForBackendStatus: stopPolling
};
