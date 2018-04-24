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
import {ReportsActions} from 'components/Reports/store/ReportsStore';
import {connect} from 'react-redux';
import T from 'i18n-react';

const PREFIX = 'features.Reports.Customizer';

const OPTIONS = [
  'namespace',
  'status',
  'start',
  'end',
  'duration',
  'user',
  'startMethod',
  'runtimeArgs',
  'numLogWarnings',
  'numLogErrors',
  'numRecordsOut'
];

ColumnsSelectorView.propTypes = {
  onClick: PropTypes.func
};

OPTIONS.forEach((option) => {
  ColumnsSelectorView.propTypes[option] = PropTypes.bool;
});

function ColumnsSelectorView(props) {
  return (
    <div className="columns-selector">
      <div className="title">
        Select Columns
      </div>

      {
        OPTIONS.map((option) => {
          return (
            <div
              className="option"
              onClick={props.onClick.bind(this, option)}
            >
              <IconSVG name={props[option] ? 'icon-check-square' : 'icon-square-o'} />

              {T.translate(`${PREFIX}.Options.${option}`)}
            </div>
          );
        })
      }
    </div>
  );
}

const mapStateToProps = (state) => {
  let obj = {};

  OPTIONS.forEach((option) => {
    obj[option] = state.customizer[option];
  });

  return obj;
};

const mapDispatch = (dispatch) => {
  return {
    onClick: (option) => {
      dispatch({
        type: ReportsActions.toggleCustomizerOption,
        payload: {
          type: option
        }
      });
    }
  };
};

const ColumnsSelector = connect(
  mapStateToProps,
  mapDispatch
)(ColumnsSelectorView);

export default ColumnsSelector;

