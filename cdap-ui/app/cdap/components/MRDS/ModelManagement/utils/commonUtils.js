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
