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
  BACKENDSTATUS,
} from 'components/LoadingIndicator/LoadingIndicatorStore';
import StatusAlertMessageStore from 'components/StatusAlertMessage/StatusAlertMessageStore';
import Cookies from 'universal-cookie';
import isNil from 'lodash/isNil';
import { Observable } from 'rxjs/Observable';
import SystemServicesStore, { pollSystemServices, stopSystemServicesPolling } from 'services/SystemServicesStore';
import SessionTokenStore from 'services/SessionTokenStore';

let pollingObservable;
let systemServiceSubscription;
let retries = 0;
const BACKEND_STATUS_POLL_INTERVAL = 10000;
const MAX_RETRIES_BEFORE_SHOWING_ERROR_IN_UI = 3;
const cookie = new Cookies();

const parseAndDispatchBackendStatus = (response) => {
  let loadingState = LoadingIndicatorStore.getState().loading;
  if (response.status <= 399) {
    if ([BACKENDSTATUS.NODESERVERDOWN].indexOf(loadingState.status) !== -1) {
      LoadingIndicatorStore.dispatch({
        type: BACKENDSTATUS.STATUSUPDATE,
        payload: {
          status: BACKENDSTATUS.BACKENDUP,
        },
      });
      retries = 0;
    }
    if ([BACKENDSTATUS.BACKENDDOWN].indexOf(loadingState.status) !== -1) {
      StatusAlertMessageStore.dispatch({
        type: 'VIEWUPDATE',
        payload: {
          view: true,
        },
      });
      LoadingIndicatorStore.dispatch({
        type: BACKENDSTATUS.STATUSUPDATE,
        payload: {
          status: BACKENDSTATUS.BACKENDUP,
        },
      });
      retries = 0;
    }
  }
  /*
    We handle,
      - 1XX Information
      - 2XX Success
      - 3XX Redirection Status codes

    We wait for the backend down error to resolve itself.
    We poll every 10 seconds for /backendstatus endpoint as hearbeat api

    On 200 response we reset the retries

    On > 399 response we retry for 3 times (total of 30 seconds wait time)
    After 3 retries if the backend is still down we show the error message in
    the UI. Otherwise if it resolves itself (like 502 gateway error) then we do nothing.
  */
  if (retries < MAX_RETRIES_BEFORE_SHOWING_ERROR_IN_UI) {
    retries += 1;
    return;
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
        status: BACKENDSTATUS.BACKENDDOWN,
      },
    });
  }
};

const dispatchNodeServerDown = () => {
  LoadingIndicatorStore.dispatch({
    type: BACKENDSTATUS.STATUSUPDATE,
    payload: {
      status: BACKENDSTATUS.NODESERVERDOWN,
    },
  });
};

const getRequestInfo = () => {
  let headers = {
    sessionToken: SessionTokenStore.getState()
  };
  let requestInfo = {
    credentials: 'include',
  };
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.get('CDAP_Auth_Token');
    if (!isNil(token)) {
      headers.Authorization = 'Bearer ' + token;
    }
    requestInfo.headers = headers;
  }
  return requestInfo;
};

const startServicePolling = () => {
  systemServiceSubscription = SystemServicesStore.subscribe(() => {
    let { list: services } = SystemServicesStore.getState().services;
    services = services.filter((service) => service.status === 'NOTOK');
    if (services.length) {
      LoadingIndicatorStore.dispatch({
        type: BACKENDSTATUS.STATUSUPDATE,
        payload: {
          services,
        },
      });
    }
  });
};

const startPolling = () => {
  stopPolling();
  pollSystemServices();
  startServicePolling();
  pollingObservable = Observable.interval(BACKEND_STATUS_POLL_INTERVAL)
    .mergeMap(() =>
      Observable.fromPromise(fetch('/backendstatus', getRequestInfo())).catch((error) => {
        dispatchNodeServerDown();
        return Observable.of(`Error: ${error}`);
      })
    )
    .subscribe(parseAndDispatchBackendStatus, dispatchNodeServerDown);
};

const stopServicePolling = () => {
  if (systemServiceSubscription) {
    systemServiceSubscription();
    systemServiceSubscription = null;
  }
  stopSystemServicesPolling();
};

const stopPolling = () => {
  stopServicePolling();
  if (pollingObservable) {
    pollingObservable.unsubscribe();
  }
};

export default {
  startPollingForBackendStatus: startPolling,
  stopPollingForBackendStatus: stopPolling,
};
