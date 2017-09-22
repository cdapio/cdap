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

import PropTypes from 'prop-types';

import React from 'react';
import IconSVG from 'components/IconSVG';

export default function FastActionButton({icon, action, disabled, id, iconClasses}) {
  let preventPropagation = (event) => {
    event.preventDefault();
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  };

  let onButtonClick = (event) => {
    preventPropagation(event);
    action(event);
  };

  return (
    // have to create a wrapper, because disabled elements don't fire mouse events in Firefox
    // also set tooltip ID on this wrapper, so that the tooltip shows up even on a disabled button
    <span
      onClick={preventPropagation.bind(this)}
      id={id}
    >
      <button
        className="btn btn-link"
        disabled={disabled}
        onClick={onButtonClick.bind(this)}
      >
        <IconSVG
          name={icon}
          className={iconClasses}
        />
      </button>
    </span>
  );
}

FastActionButton.propTypes = {
  icon: PropTypes.string,
  action: PropTypes.func,
  disabled: PropTypes.bool,
  id: PropTypes.string,
  iconClasses: PropTypes.string
};
