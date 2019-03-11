import { isNil } from 'lodash';

export function getEpochDateString(params) {
  if(!isNil(params)) {
    if (isNaN(params.value)) {
      return "—";
    } else {
      let date = new Date(params.value * 1000);
      return date.toDateString();
    }
  }
  return "—";
}