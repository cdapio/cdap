/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { setS3Search } from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import T from 'i18n-react';
import TableContents from 'components/DataPrep/DataPrepBrowser/S3Browser/BucketData/TableContents';
import If from 'components/If';

const PREFIX = 'features.DataPrep.DataPrepBrowser.S3Browser.BucketData';
const props = {
  clearSearch: PropTypes.func,
  data: PropTypes.arrayOf(PropTypes.object),
  search: PropTypes.string,
  loading: PropTypes.bool,
  enableRouting: PropTypes.bool,
  prefix: PropTypes.string,
  onWorkspaceCreate: PropTypes.func,
};

const TableHeader = ({ enableRouting }) => {
  if (enableRouting) {
    return (
      <div className="row">
        <div className="col-3">{T.translate(`${PREFIX}.Headers.Name`)}</div>
        <div className="col-3">{T.translate(`${PREFIX}.Headers.Owner`)}</div>
        <div className="col-3">{T.translate(`${PREFIX}.Headers.Size`)}</div>
        <div className="col-3">{T.translate(`${PREFIX}.Headers.LastModified`)}</div>
      </div>
    );
  }
  return (
    <div className="row">
      <div className="col-12">{T.translate(`${PREFIX}.Headers.Name`)}</div>
    </div>
  );
};
TableHeader.propTypes = props;

const BucketData = ({
  data,
  search,
  clearSearch,
  loading,
  prefix,
  enableRouting,
  onWorkspaceCreate,
}) => {
  if (loading) {
    return <LoadingSVGCentered />;
  }

  const filteredData = data.filter((d) => {
    if (search && search.length && d.name) {
      let isSearchTextInName = d.name.indexOf(search);
      if (d.type && d.type === 'bucket') {
        return isSearchTextInName !== -1 || (d.owner && d.owner.indexOf(search) !== -1);
      }
      return isSearchTextInName !== -1 || (d.path && d.path.indexOf(search) !== -1);
    }
    return true;
  });

  return (
    <div>
      <If condition={filteredData.length}>
        <div className="s3-content-header">
          <TableHeader enableRouting={enableRouting} />
        </div>
      </If>
      <TableContents
        search={search}
        clearSearch={clearSearch}
        data={filteredData}
        prefix={prefix}
        enableRouting={enableRouting}
        onWorkspaceCreate={onWorkspaceCreate}
      />
    </div>
  );
};

BucketData.propTypes = props;

const mapStateToProps = (state, ownProps) => {
  let { enableRouting = true, onWorkspaceCreate = () => {} } = ownProps;
  return {
    data: state.s3.activeBucketDetails,
    search: state.s3.search,
    loading: state.s3.loading,
    prefix: state.s3.prefix,
    enableRouting,
    onWorkspaceCreate,
  };
};

const mapDispatchToProps = () => {
  return {
    clearSearch: () => {
      setS3Search('');
    },
  };
};

const BucketDataWrapper = connect(
  mapStateToProps,
  mapDispatchToProps
)(BucketData);

export default BucketDataWrapper;
