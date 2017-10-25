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
import T from 'i18n-react';
import React from 'react';
import {connect} from 'react-redux';

const ListingInfo = ({bucketData, loading}) => {
  if (loading) {
    return <span>.</span>;
  }
  let dirsCount = bucketData.filter(file => file.directory).length;
  let filesCount = bucketData.length - dirsCount;
  return <span> {T.translate('features.DataPrep.DataPrepBrowser.S3Browser.TopPanel.ListingInfo.label', {filesCount, dirsCount})} </span>;
};

ListingInfo.propTypes = {
  bucketData: PropTypes.arrayOf(PropTypes.object),
  loading: PropTypes.bool
};

const mapStateToProps = (state) => {
  return {
    bucketData: state.s3.activeBucketDetails,
    loading: state.s3.loading
  };
};

const ListInfoWrapper = connect(mapStateToProps)(ListingInfo);

export default ListInfoWrapper;
