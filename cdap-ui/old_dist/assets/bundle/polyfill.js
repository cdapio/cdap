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
 * getTransformToElement
 * This function is deprecated in chrome 48. Dagre-D3 is using this function to
 * draw the edge connections.
 *
 * Dagre-D3 issue: https://github.com/cpettitt/dagre-d3/issues/202
 **/

SVGElement.prototype.getTransformToElement = SVGElement.prototype.getTransformToElement || function(toElement) {
  return toElement.getScreenCTM().inverse().multiply(this.getScreenCTM());
};

// 'includes' function of String is not available in older version of chromium browsers.
if (!String.prototype.includes) {
  String.prototype.includes = function() {'use strict';
    return String.prototype.indexOf.apply(this, arguments) !== -1;
  };
}
// 'assign' function of Object is not available in older version of chromium browsers.
if (typeof Object.assign !== 'function') {
  Object.assign = function(target) {
    'use strict';
    if (target === null) {
      throw new TypeError('Cannot convert undefined or null to object');
    }

    target = Object(target);
    for (var index = 1; index < arguments.length; index++) {
      var source = arguments[index];
      if (source !== null) {
        for (var key in source) {
          if (Object.prototype.hasOwnProperty.call(source, key)) {
            target[key] = source[key];
          }
        }
      }
    }
    return target;
  };
}
