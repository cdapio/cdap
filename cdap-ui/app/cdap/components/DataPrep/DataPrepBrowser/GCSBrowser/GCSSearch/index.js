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

import PropTypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';
import { Input } from 'reactstrap';
import { setGCSSearch } from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import T from 'i18n-react';

const SearchBox = ({ search, onChange }) => {
  return (
    <Input
      placeholder={T.translate('features.DataPrep.DataPrepBrowser.GCSBrowser.Search.placeholder')}
      value={search}
      onChange={onChange}
      data-cy="gcs-search-box"
    />
  );
};

SearchBox.propTypes = {
  search: PropTypes.string,
  onChange: PropTypes.func,
};

const mapStateToProps = (state) => {
  return {
    search: state.gcs.search,
  };
};
const mapDispatchToProps = () => {
  return {
    onChange: (e) => {
      setGCSSearch(e.target.value);
    },
  };
};

const GCSSearch = connect(mapStateToProps, mapDispatchToProps)(SearchBox);
export default GCSSearch;
