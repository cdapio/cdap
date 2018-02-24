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
import {Observable} from 'rxjs/Observable';
import {Subject} from 'rxjs/Subject';
import 'rxjs/add/observable/combineLatest';
import 'rxjs/add/observable/throw';
import 'rxjs/add/observable/of';
import 'rxjs/add/observable/fromEvent';
import 'rxjs/add/observable/merge';
import 'rxjs/add/observable/interval';
import 'rxjs/add/observable/fromPromise';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/combineLatest';
import 'rxjs/add/operator/mergeMap';
import 'rxjs/add/operator/map';

export default class Datasource {
  constructor(genericResponseHandlers = [() => true]) {
    let socketData = Socket.getObservable();
    this.bindings = {};

    this.socketSubscription =  socketData.subscribe(
      (data) => {
        let hash = data.resource.id;
        if (!this.bindings[hash]) { return; }

        genericResponseHandlers.forEach(handler => handler(data));

        if (data.statusCode > 299 || data.warning) {
          this.bindings[hash].rx.error({
            statusCode: data.statusCode,
            response: data.response || data.body || data.error
          });
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
      }
    );
  }

  request(resource = {}) {
    let generatedResource = {
      id: uuidV4(),
      json: resource.json === false ? false : true,
      method: resource.method || 'GET',
      suppressErrors: resource.suppressErrors || false
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
      resource.url = Datasource.constructUrl(resource);
    }

    generatedResource.url = this.buildUrl(resource.url, resource.params);

    let subject = new Subject();

    this.bindings[generatedResource.id] = {
      rx: subject,
      resource: generatedResource,
      type: 'REQUEST'
    };

    Socket.send({
      action: 'request',
      resource: generatedResource
    });

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
      resource.url = Datasource.constructUrl(resource);
    }

    generatedResource.url = this.buildUrl(resource.url, resource.params);

    let subject = new Subject();

    let observable = Observable.create((obs) => {
      subject.subscribe(
        (data) => {
          obs.next(data);
        },
        (err) => {
          obs.error(err);
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
      type: 'POLL'
    };

    Socket.send({
      action: 'poll-start',
      resource: generatedResource
    });

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
        resource: this.bindings[id].resource
      });

      this.bindings[id].rx.unsubscribe();
      delete this.bindings[id];
    }
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

  static constructUrl = (resource) => {

    let url;

    // further sugar for building absolute url
    if (resource._cdapPath) {
      url = [
        window.CDAP_CONFIG.sslEnabled ? 'https://' : 'http://',
        window.CDAP_CONFIG.cdap.routerServerUrl,
        ':',
        window.CDAP_CONFIG.sslEnabled ? window.CDAP_CONFIG.cdap.routerSSLServerPort : window.CDAP_CONFIG.cdap.routerServerPort,
        '/v3',
        resource._cdapPath
      ].join('');

      delete resource._cdapPath;
    }

    return url;
  };

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
      return encodeURIComponent(val).
             replace(/%40/gi, '@').
             replace(/%3A/gi, ':').
             replace(/%24/g, '$').
             replace(/%2C/gi, ',').
             replace(/%3B/gi, ';').
             replace(/%20/g, (pctEncodeSpaces ? '%20' : '+'));
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
      url += ((url.indexOf('?') === -1) ? '?' : '&') + parts.join('&');
    }
    return url;
  }
}
