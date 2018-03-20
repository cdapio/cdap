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
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.RuntimeArgs.ProvidedPopover';

const mapStateToProps = (state, ownProps) => {
  return {
    containsMacros: Object.keys(state.resolvedMacros).length > 0,
    toggleAllProvided: ownProps.toggleAllProvided,
    disabled: ownProps.disabled
  };
};

const ProvidedPopover = ({containsMacros, toggleAllProvided, disabled}) => {
  if (!containsMacros || disabled) { return null; }

  return (
    <span>
      <span className="provided-label">
        {T.translate(`${PREFIX}.provided`)}
      </span>
      <Popover
        target={() => <IconSVG name="icon-caret-square-o-down" />}
        placement="bottom"
        className="provided-popover"
      >
        <ul>
          <li onClick={toggleAllProvided.bind(null, false)}>
            {T.translate(`${PREFIX}.clearAll`)}
          </li>
          <li onClick={toggleAllProvided.bind(null, true)}>
            {T.translate(`${PREFIX}.selectAll`)}
          </li>
        </ul>
      </Popover>
    </span>
  );
};

ProvidedPopover.propTypes = {
  containsMacros: PropTypes.bool,
  toggleAllProvided: PropTypes.func,
  disabled: PropTypes.bool
};

const ConnectedProvidedPopover = connect(mapStateToProps)(ProvidedPopover);

export default ConnectedProvidedPopover;
