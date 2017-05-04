/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import T from 'i18n-react';

export default class CutMenuItem extends Component {
  constructor(props) {
    super(props);
    this.highlightColumn = this.highlightColumn.bind(this);
  }
  highlightColumn() {
    let {highlightColumns} = DataPrepStore.getState().dataprep;
    DataPrepStore.dispatch({
      type: DataPrepActions.setHighlightColumns,
      payload: {
        highlightColumns: {
          columns: highlightColumns.columns.concat([this.props.column]),
          directive: 'CUT'
        }
      }
    });
    this.props.onComplete();
  }
  render() {
    return (
      <div
        className="cut-menu-item option clearfix"
        onClick={this.highlightColumn}
      >
        <span>
          {T.translate('features.DataPrep.Directives.CutMenuItem.menuLabel')}
        </span>
      </div>
    );
  }
}
CutMenuItem.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func
};
