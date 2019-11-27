/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import Socket from '../socket';
import uuidV4 from 'uuid/v4';
import ee from 'event-emitter';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import 'rxjs/add/observable/combineLatest';
import 'rxjs/add/observable/throw';
import 'rxjs/add/observable/of';
import 'rxjs/add/observable/fromEvent';
import 'rxjs/add/observable/merge';
import 'rxjs/add/observable/interval';
import 'rxjs/add/observable/fromPromise';
import 'rxjs/add/observable/forkJoin';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/combineLatest';
import 'rxjs/add/operator/mergeMap';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/debounceTime';
import WindowManager, {WINDOW_ON_BLUR, WINDOW_ON_FOCUS} from 'services/WindowManager';

var CDAP_API_VERSION = 'v3';
// FIXME (CDAP-14836): Right now this is scattered across node and client. Need to consolidate this.
const REQUEST_ORIGIN_ROUTER = 'ROUTER';

export default class Datasource {
  constructor(genericResponseHandlers = [() => true]) {
    this.eventEmitter = ee(ee);
    let socketData = Socket.getObservable();
    this.bindings = {};

    this.socketSubscription = socketData.subscribe((data) => {
      let hash = data.resource.id;
      if (!this.bindings[hash]) {
        return;
      }

      genericResponseHandlers.forEach((handler) => handler(data));

      if (data.statusCode > 299 || data.warning) {
        /**
         * There is an issue here. When backend goes down we stop all the poll
         * and inspite of stopping all polling calls and unsubscribing all subscribers
         * we still get the rx.error call which tries to set the observers length to 0
         * and errors out. This doesn't harm us today as when system comes up we refresh
         * the UI and everything loads.
         *
         * This is being wrapped in a try catch block in case the subscriber do not define
         * an error callback. Without this, the error will bubble up as an uncaught error
         * and terminate the socketData subscriber.
         */
        try {
          this.bindings[hash].rx.error({
            statusCode: data.statusCode,
            response: data.response || data.body || data.error,
          });
        } catch (e) {
          console.groupCollapsed('Error: ' + data.resource.url);
          console.log('Resource', data.resource);
          console.log('Error', e);
          console.groupEnd();
        }
      } else {
        this.bindings[hash].rx.next(data.response);
      }

      // Adding check if bindings[hash] exist because if a Poll that gets cancelled
      // within 1 tick, the bindings[hash] will already be deleted
      if (this.bindings[hash] && this.bindings[hash].type === 'REQUEST') {
        this.bindings[hash].rx.complete();
        this.bindings[hash].rx.unsubscribe();
        delete this.bindings[hash];
      }
    });


    /**
     * On socket reconnect, go through the the existing bindings, and resend the requests
     * to the websocket. The original request bindings will still be preserved.
     */
    this.eventEmitter.on('SOCKET_RECONNECT', () => {
      Object.keys(this.bindings).forEach((reqId) => {
        const req = this.bindings[reqId];
        if (!req) {
          return;
        }

        if (req.type === 'REQUEST') {
          this.socketSend('request', req.resource);
        } else if (req.type === 'POLL') {
          this.socketSend('poll-start', req.resource);
        }
      });
    });
    this.eventEmitter.on(WINDOW_ON_FOCUS, this.resumePoll.bind(this));
    this.eventEmitter.on(WINDOW_ON_BLUR, this.pausePoll.bind(this));
  }

  socketSend(actionType, resource) {
    Socket.send({
      action: actionType,
      resource: resource,
    });
  }

  request(resource = {}) {
    let generatedResource = {
      id: resource.id || uuidV4(),
      json: resource.json === false ? false : true,
      method: resource.method || 'GET',
      suppressErrors: resource.suppressErrors || false,
    };

    if (resource.body) {
      generatedResource.body = resource.body;
    }
    if (resource.data) {
      generatedResource.body = resource.data;
    }
    if (resource.headers) {
      generatedResource.headers = resource.headers;
    }
    if (resource.contentType) {
      generatedResource.headers['Content-Type'] = resource.contentType;
    }
    if (!resource.url) {
      resource.url = resource._cdapPath;
      delete resource._cdapPath;
    }
    let apiVersion = resource.apiVersion || CDAP_API_VERSION;
    if (!resource.requestOrigin || resource.requestOrigin === REQUEST_ORIGIN_ROUTER) {
      resource.url = `/${apiVersion}${resource.url}`;
    }
    generatedResource.url = this.buildUrl(resource.url, resource.params);

    if (resource.requestOrigin) {
      generatedResource.requestOrigin = resource.requestOrigin;
    }

    let subject = new Subject();

    this.bindings[generatedResource.id] = {
      rx: subject,
      resource: generatedResource,
      type: 'REQUEST',
    };

    this.socketSend('request', generatedResource);

    return subject;
  }

  poll(resource = {}) {
    let generatedResource = {
      id: uuidV4(),
      interval: resource.interval || 10000,
      json: resource.json || true,
      method: resource.method || 'GET',
      suppressErrors: resource.suppressErrors || false,
    };

    if (resource.body) {
      generatedResource.body = resource.body;
    }
    if (resource.data) {
      generatedResource.body = resource.data;
    }
    if (resource.headers) {
      generatedResource.headers = resource.headers;
    }

    if (!resource.url) {
      resource.url = resource._cdapPath;
      delete resource._cdapPath;
    }

    let apiVersion = resource.apiVersion || CDAP_API_VERSION;
    if (!resource.requestOrigin || resource.requestOrigin === REQUEST_ORIGIN_ROUTER) {
      resource.url = `/${apiVersion}${resource.url}`;
    }

    generatedResource.url = this.buildUrl(resource.url, resource.params);

    if (resource.requestOrigin) {
      generatedResource.requestOrigin = resource.requestOrigin;
    }
    let subject = new Subject();

    let observable = Observable.create((obs) => {
      subject.subscribe(
        (data) => {
          obs.next(data);
        },
        (err) => {
          try {
            obs.error(err);
          } catch (e) {
            console.groupCollapsed('Error: ' + data.resource.url);
            console.log('Resource', data.resource);
            console.log('Error', e);
            console.groupEnd();
          }
        }
      );

      return () => {
        this.stopPoll(generatedResource.id);
        subject.unsubscribe();
      };
    });

    this.bindings[generatedResource.id] = {
      rx: subject,
      resource: generatedResource,
      type: 'POLL',
    };

    this.socketSend('poll-start', generatedResource);

    return observable;
  }

  stopPoll(resourceId) {
    let id;

    if (typeof resourceId === 'object' && resourceId !== null) {
      id = resourceId.params.pollId;
    } else {
      id = resourceId;
    }

    if (this.bindings[id]) {
      Socket.send({
        action: 'poll-stop',
        resource: this.bindings[id].resource,
      });

      this.bindings[id].rx.unsubscribe();
      delete this.bindings[id];
    }
  }

  pausePoll() {
    Object.keys(this.bindings)
      .filter(subscriptionID => this.bindings[subscriptionID].action === 'POLL')
      .forEach(subscriptionID => {
        Socket.send({
          action: 'poll-stop',
          resource: this.bindings[subscriptionID].resource,
        });
      });
  }

  resumePoll() {
    Object.keys(this.bindings)
      .filter(subscriptionID => this.bindings[subscriptionID].action === 'POLL')
      .forEach(subscriptionID => {
        Socket.send({
          action: 'poll-start',
          resource: this.bindings[subscriptionID].resource,
        });
      });
  }

  destroy() {
    this.socketSubscription.unsubscribe();

    // stopping existing polls
    for (let key in this.bindings) {
      if (this.bindings[key].type === 'POLL') {
        this.stopPoll(this.bindings[key].resource);
      }
    }
    this.bindings = {};
  }

  buildUrl(url, params = {}) {
    if (!params) {
      return url;
    }
    var parts = [];

    function forEachSorted(obj, iterator, context) {
      var keys = Object.keys(params).sort();
      keys.forEach((key) => {
        iterator.call(context, obj[key], key);
      });
      return keys;
    }

    function encodeUriQuery(val, pctEncodeSpaces) {
      return encodeURIComponent(val)
        .replace(/%40/gi, '@')
        .replace(/%3A/gi, ':')
        .replace(/%24/g, '$')
        .replace(/%2C/gi, ',')
        .replace(/%3B/gi, ';')
        .replace(/%20/g, pctEncodeSpaces ? '%20' : '+');
    }

    forEachSorted(params, function(value, key) {
      if (value === null || typeof value === 'undefined') {
        return;
      }
      if (!Array.isArray(value)) {
        value = [value];
      }

      value.forEach((v) => {
        if (typeof v === 'object' && v !== null) {
          if (value.toString() === '[object Date]') {
            v = v.toISOString();
          } else {
            v = JSON.stringify(v);
          }
        }
        parts.push(encodeUriQuery(key) + '=' + encodeUriQuery(v));
      });
    });
    if (parts.length > 0) {
      url += (url.indexOf('?') === -1 ? '?' : '&') + parts.join('&');
    }
    return url;
  }
}
