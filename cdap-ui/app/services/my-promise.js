/*
 * Copyright © 2015 Cask Data, Inc.
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

/*
  Purpose:
    TL;DR
    MyPromise is observable promise. Sounds very disturbing but this is the initial
    attempt at sockets + $resource in an angular app.

    Longer Version:
    We cannot use promise pattern in a socket environment as promises
    resolve only once. In the case of sockets we might want to 'poll' for a data
    and get updated as soon something has changed.
    MyPromise provides an interface simillar to a Promise (not $q) and accepts
    in addition to a function a second argument. If you create your promise to
    be an observable then the handlers are never erased and your callback/resolve
    handler will be called whenever the promise gets resolved.

    @param {function} which gets a 'resolve' and a 'reject' methods
    @param {boolean} is observable or not.

    PS: Inspired from
      - https://www.promisejs.org/implementing/
      - https://github.com/kriskowal/q/blob/v1/design/README.js
*/
angular.module(PKG.name + '.services')
  .provider('MyPromise', function() {
    var PENDING = 0;
    var FULFILLED = 1;
    var REJECTED = 2;
    var UPDATED = 3;


    function Promise(fn, isObservable) {
      var isObserve = isObservable;
      // store state which can be PENDING, FULFILLED or REJECTED
      var state = PENDING;

      // store value once FULFILLED or REJECTED
      var value = null;

      // store sucess & failure handlers
      var handlers = [];

      function fulfill(result) {
        state = FULFILLED;
        value = result;
        handlers.forEach(handle);
        if (!isObserve) {
          handlers = null;
        }
      }

      function reject(error) {
        state = REJECTED;
        value = error;
        handlers.forEach(handle);
        if (!isObserve) {
          handlers = null;
        }
      }

      function resolve(result) {
        try {
          var then = getThen(result);
          if (then) {
            doResolve(then.bind(result), resolve, reject);
            return;
          }
          fulfill(result);
        } catch (ex) {
          if (console.error) {
            console.error(ex);
          } else {
            console.log(ex);
          }
          reject(ex);
        }
      }

      function handle(handler) {
        if (state === PENDING) {
          handlers.push(handler);
        } else {
          if ((state === FULFILLED || state === UPDATED) &&
            typeof handler.onFulfilled === 'function') {
            handler.onFulfilled(value);
            if (isObserve) {
              state = UPDATED;
            }
          }
          if (state === REJECTED &&
            typeof handler.onRejected === 'function') {
            handler.onRejected(value);
          }
        }
      }

      this.done = function (onFulfilled, onRejected) {
        handle({
          onFulfilled: onFulfilled,
          onRejected: onRejected
        });
      };


      this.then = function (onFulfilled, onRejected) {
        var self = this;
        // Return a new promise for chaining.
        return new Promise(function (resolve, reject) {
          return self.done(function (result) {
            if (typeof onFulfilled === 'function') {
              try {
                return resolve(onFulfilled(result));
              } catch (ex) {
                if (console.error) {
                  console.error(ex);
                } else {
                  console.log(ex);
                }
                return reject(ex);
              }
            } else {
              return resolve(result);
            }
          }, function (error) {
            if (typeof onRejected === 'function') {
              try {
                return resolve(onRejected(error));
              } catch (ex) {
                if (console.error) {
                  console.error(ex);
                } else {
                  console.log(ex);
                }
                return reject(ex);
              }
            } else {
              if (console.error) {
                console.error(error);
              } else {
                console.log(error);
              }
              return reject(error);
            }
          });
        }, isObserve);
      };

      doResolve(fn, resolve, reject);

      function getThen(value) {
        var t = typeof value;
        if (value && (t === 'object' || t === 'function')) {
          var then = value.then;
          if (typeof then === 'function') {
            return then;
          }
        }
        return null;
      }

      /**
       * Take a potentially misbehaving resolver function and make sure
       * onFulfilled and onRejected are only called once.
       *
       * Makes no guarantees about asynchrony.
       *
       * @param {Function} fn A resolver function that may not be trusted
       * @param {Function} onFulfilled
       * @param {Function} onRejected
       */
      function doResolve(fn, onFulfilled, onRejected) {
        var done = false;
        try {
          fn(function (value) {
            if (!isObserve) {
              if (done) {
                return;
              }
              done = true;
            }
            onFulfilled(value);
          }, function (reason) {
            if (!isObserve) {
              if (done) {
                return;
              }
              done = true;
            }
            onRejected(reason);
          });
        } catch (ex) {
          if (console.error) {
            console.error(ex);
          } else {
            console.log(ex);
          }
          if (done) {
            return;
          }
          done = true;
          onRejected(ex);
        }
      }

    }


    this.$get = function() {
      return Promise;
    };
  });
