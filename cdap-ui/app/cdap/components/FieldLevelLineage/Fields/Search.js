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
import IconSVG from 'components/IconSVG';
import {search} from 'components/FieldLevelLineage/store/ActionCreator';

function FieldSearchView({searchValue}) {
  return (
    <div className="search-container">
      <div className="input-group">
        <input
          type="text"
          className="form-control"
          placeholder="Search by field name"
          onChange={search}
          value={searchValue}
        />
        <span className="input-group-addon">
          <IconSVG name="icon-search" />
        </span>
      </div>
    </div>
  );
}

FieldSearchView.propTypes = {
  searchValue: PropTypes.string,
  onChange: PropTypes.func
};

const mapStateToProps = (state) => {
  return {
    searchValue: state.lineage.search
  };
};

const FieldSearch = connect(
  mapStateToProps
)(FieldSearchView);

export default FieldSearch;
