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
import IconSVG from 'components/IconSVG';
import {connect} from 'react-redux';

function StatusViewerView({selections}) {
  let text = 'Select one';

  if (selections.length > 0) {
    text = selections.join(', ');
  }

  return (
    <div className="status-viewer">
      <div className="status-text">
        {text}
      </div>

      <div className="caret-dropdown">
        <IconSVG name="icon-caret-down" />
      </div>
    </div>
  );
}

StatusViewerView.propTypes = {
  selections: PropTypes.array
};

const mapStateToProps = (state) => {
  return {
    selections: state.status.statusSelections
  };
};

const StatusViewer = connect(
  mapStateToProps
)(StatusViewerView);

export default StatusViewer;
