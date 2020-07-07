/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';
require('./ToggleSwitch.scss');

export default function ToggleSwitch({
  isOn,
  onToggle,
  disabled,
  onLabel,
  offLabel,
  dataCy = null,
}) {
  return (
    <div className={classnames('toggle-switch-container', { disabled: disabled })}>
      <div
        className={classnames('toggle-switch', { on: isOn, off: !isOn })}
        onClick={onToggle}
        data-cy={`switch-${dataCy}`}
      >
        <div className="switch-button" />
        <div
          className={classnames('label', 'truncate', { 'on-label': isOn, 'off-label': !isOn })}
          data-cy={isOn ? `toggle-${onLabel}` : `toggle-${offLabel}`}
        >
          {isOn ? onLabel : offLabel}
        </div>
      </div>
    </div>
  );
}

ToggleSwitch.defaultProps = {
  onLabel: 'On',
  offLabel: 'Off',
};

ToggleSwitch.propTypes = {
  isOn: PropTypes.bool,
  onToggle: PropTypes.func,
  disabled: PropTypes.bool,
  onLabel: PropTypes.string,
  offLabel: PropTypes.string,
  dataCy: PropTypes.string,
};
