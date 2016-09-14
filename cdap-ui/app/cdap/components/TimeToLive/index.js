/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes} from 'react';
import { Input, Label } from 'reactstrap';
require('./TimeToLive.less');
import shortid from 'shortid';

export default function TimeToLive({value, onChange}) {
  const getOptions = (count) => {
    return Array.apply(null, {length: count})
      .map((e, i) => (
        <option key={shortid.generate()}>
          {i}
        </option>
      ));
  };
  const [months=0, days=0, hours=0, mins=0, sec=0] = value.split(',');
  const localOnChange = (type, e) => {
    let state = [months, days, hours, mins, sec];
    let stateMap = {months: 0, days: 1, hours: 2, mins: 3, sec: 4};
    state[stateMap[type]] = e.target.value;
    // FIXME: How can I not do this? Looks ........ bad.
    e.target.ttlValue = state.join(',');
    onChange.call(null, e);
  };
  return (
    <div className="cask-time-to-live">
      <div>
        <div className="control-label">
          <Label>Months</Label>
        </div>
        <Input
          type="select"
          size="sm"
          value={months}
          onChange={localOnChange.bind(null, 'months')}
        >
          {getOptions(100)}
        </Input>
      </div>

      <div>
        <div className="control-label">
          <Label>Days</Label>
        </div>
        <Input
          type="select"
          size="sm"
          value={days}
          onChange={localOnChange.bind(null, 'days')}
        >
          {getOptions(100)}
        </Input>
      </div>
      <div>
        <div className="control-label">
          <Label>Hours</Label>
        </div>
        <Input
          type="select"
          size="sm"
          value={hours}
          onChange={localOnChange.bind(null, 'hours')}
        >
          {getOptions(100)}
        </Input>
      </div>
      <div>
        <div className="control-label">
          <Label>Mins</Label>
        </div>
        <Input
          type="select"
          size="sm"
          value={mins}
          onChange={localOnChange.bind(null, 'mins')}
        >
          {getOptions(100)}
        </Input>
      </div>
      <div>
        <div className="control-label">
          <Label>Sec</Label>
        </div>
        <Input
          type="select"
          size="sm"
          value={sec}
          onChange={localOnChange.bind(null, 'sec')}
        >
          {getOptions(100)}
        </Input>
      </div>
    </div>
  );
}

TimeToLive.propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func
};
