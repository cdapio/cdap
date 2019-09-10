/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import * as momentTimeZone from 'moment-timezone';
import { isNil } from 'lodash';

export function timeFormatter(params) {
    let dateTimeFormat = 'DD-MM-YYYY HH:mm';
    let date = getMomentDateFor(params.value, 'GMT');
    return isNil(date) ? "" : date.format(dateTimeFormat);
  }


  export function getMomentDateFor(epoch, timezone) {
    if (isNil(epoch)) {
      return undefined;
    }
    return momentTimeZone.tz(epoch, timezone);
  }


/* This function convert a string to object for one level hirarchy */
  export function stringToObj(value) {
    const val = value.slice(1, -1).split(',');
    value = {};
    for (let i = 0; i < val.length; i++) {
      const tmp = val[i].split('=');
      value[String(tmp[0]).trim()] = String(tmp[1]).trim();
    }
    return value;
  }

  export function hyperParameterFormater(params) {
      const ValueOb= stringToObj(params.value);
      return ValueOb['default'];
  }

  export function metricParameterFormater(params) {
    const ValueOb= stringToObj(params.value);
    const ss = params.colDef.headerName.split('_');
    const field = ss.length>0 ? ss[ss.length-1] : "";
    return ValueOb[field];
}
