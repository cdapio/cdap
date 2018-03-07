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
import {connect} from 'react-redux';
import Popover from 'components/Popover';
import IconSVG from 'components/IconSVG';

const mapStateToProps = (state, ownProps) => {
  return {
    containsMacros: Object.keys(state.resolvedMacros).length > 0,
    toggleAllProvided: ownProps.toggleAllProvided
  };
};

const ProvidedPopover = ({containsMacros, toggleAllProvided}) => {
  if (!containsMacros) { return null; }

  return (
    <span>
      <span className="provided-label">
        Provided
      </span>
      <Popover
        target={() => <IconSVG name="icon-caret-square-o-down" />}
        placement="bottom"
        className="provided-popover"
      >
        <ul>
          <li onClick={toggleAllProvided.bind(null, false)}>
            Clear All
          </li>
          <li onClick={toggleAllProvided.bind(null, true)}>
            Select All
          </li>
        </ul>
      </Popover>
    </span>
  );
};

ProvidedPopover.propTypes = {
  containsMacros: PropTypes.bool,
  toggleAllProvided: PropTypes.func
};

const ConnectedProvidedPopover = connect(mapStateToProps)(ProvidedPopover);

export default ConnectedProvidedPopover;
