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
import {STATUS_OPTIONS} from 'components/Reports/store/ReportsStore';
import {getStatusSelectionsLabels} from 'components/Reports/store/ActionCreator';
import T from 'i18n-react';

const PREFIX = 'features.Reports.Customizer.StatusSelector';

function StatusViewer(selections) {
  let text = T.translate(`${PREFIX}.selectOne`);
  let numSelections = selections.length;

  if (numSelections > 0) {
    if (numSelections === STATUS_OPTIONS.length) {
      text = T.translate(`${PREFIX}.allStatuses`);
    } else {
      text = getStatusSelectionsLabels(selections).join(', ');
      if (numSelections > 1) {
        text = `(${numSelections}) ` + text;
      }
    }
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

StatusViewer.propTypes = {
  selections: PropTypes.array
};

export default StatusViewer;
