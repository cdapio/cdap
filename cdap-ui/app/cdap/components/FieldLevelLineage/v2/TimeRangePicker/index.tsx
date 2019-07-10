/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { useContext, useState } from 'react';
import { ButtonDropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import { setTimeRange, setCustomTimeRange } from 'components/FieldLevelLineage/store/ActionCreator';
import { TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';
import ExpandableTimeRange from 'components/TimeRangePicker/ExpandableTimeRange';
import T from 'i18n-react';
import { IContextState, FllContext } from 'components/FieldLevelLineage/v2/Context/FllContext';

const PREFIX = 'features.FieldLevelLineage.TimeRangeOptions';

export default function TimeRangePicker() {
  const { start, end, selection } = useContext<IContextState>(FllContext);
  const [dropdownOpen, setToggleState] = useState<boolean>(false);

  const toggle = () => {
    setToggleState(!dropdownOpen);
  };

  // const renderCustomTimeRange = () => {
  //   if (this.props.selections !== TIME_OPTIONS[0]) {
  //     return null;
  //   }

  //   return (
  //     <div className="custom-time-range-container">
  //       <ExpandableTimeRange
  //         onDone={this.onDone}
  //         inSeconds={true}
  //         start={this.props.start}
  //         end={this.props.end}
  //       />
  //     </div>
  //   );
  // };

  return (
    <span>
      <ButtonDropdown isOpen={dropdownOpen} toggle={toggle}>
        <DropdownToggle caret>
          <h5>{T.translate(`${PREFIX}.${selection}`)}</h5>
        </DropdownToggle>

        <DropdownMenu>
          {TIME_OPTIONS.map((option) => {
            return (
              <DropdownItem
                key={option}
                onClick={(e) => {
                  console.log('Time range gets set in context here to ', e.target.innerText);
                }}
              >
                {T.translate(`${PREFIX}.${option}`)}
              </DropdownItem>
            );
          })}
        </DropdownMenu>
      </ButtonDropdown>

      {/* {this.renderCustomTimeRange()} */}
    </span>
  );
}
