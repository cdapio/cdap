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

export default function FastActionButton({icon, action, disabled, id}) {

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
        <span className={icon}></span>
      </button>
    </span>
  );
}


FastActionButton.propTypes = {
  icon: PropTypes.string,
  action: PropTypes.func,
  disabled: PropTypes.bool,
  id: PropTypes.string
};
