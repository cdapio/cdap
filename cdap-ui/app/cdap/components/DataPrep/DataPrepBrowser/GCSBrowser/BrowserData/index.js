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
import {connect} from 'react-redux';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {Link} from 'react-router-dom';
import {setGCSPrefix, setGCSSearch} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import {preventPropagation} from 'services/helpers';
import classnames from 'classnames';
import EmptyMessageContainer from 'components/EmptyMessageContainer';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import {humanReadableDate, convertBytesToHumanReadable, HUMANREADABLESTORAGE_NODECIMAL} from 'services/helpers';

const PREFIX = 'features.DataPrep.DataPrepBrowser.GCSBrowser.BrowserData';
const props = {
  clearSearch: PropTypes.func,
  data: PropTypes.arrayOf(PropTypes.object),
  search: PropTypes.string,
  loading: PropTypes.bool,
  enableRouting: PropTypes.bool,
  prefix: PropTypes.string,
  onWorkspaceCreate: PropTypes.func
};

const getPrefix = (file, prefix) => {

  return file.path ? file.path : `${prefix}${file.name}/`;
};

const onClickHandler = (enableRouting, onWorkspaceCreate, file, prefix, e) => {
  if (!file.directory) {
    if (file.wrangle) {
      onWorkspaceCreate(file);
    }
    preventPropagation(e);
    return false;
  }
  if (enableRouting) {
    return;
  }
  if (file.directory) {
    setGCSPrefix(getPrefix(file, prefix));
  }
  preventPropagation(e);
  return false;
};

const TableHeader = ({enableRouting}) => {
  if (enableRouting) {
    return (
      <div className="row">
        <div className="col-xs-3">
          {T.translate(`${PREFIX}.Headers.Name`)}
        </div>
        <div className="col-xs-3">
          {T.translate(`${PREFIX}.Headers.Type`)}
        </div>
        <div className="col-xs-3">
          {T.translate(`${PREFIX}.Headers.Size`)}
        </div>
        <div className="col-xs-3">
          {T.translate(`${PREFIX}.Headers.LastModified`)}
        </div>
      </div>
    );
  }
  return (
    <div className="row">
      <div className="col-xs-12">
        {T.translate(`${PREFIX}.Headers.Name`)}
      </div>
    </div>
  );
};
TableHeader.propTypes = props;

const TableContents = ({enableRouting, search, data, onWorkspaceCreate, prefix, clearSearch}) => {
  let filteredData = data.filter(d => {
    if (search && search.length) {
      let isSearchTextInName = d.name.indexOf(search);
      if (d.type === 'bucket') {
        return isSearchTextInName !== -1 || d.owner.indexOf(search) !== -1;
      }
      return isSearchTextInName !== -1 || d.path.indexOf(search) !== -1;
    }
    return true;
  });

  let ContainerElement = enableRouting ? Link : 'div';
  let pathname = window.location.pathname.replace(/\/cdap/, '');
  if (!filteredData.length) {
    return (
      <div className="gcs-buckets empty-message">
        <div className="row">
          <div className="col-xs-12">
            <EmptyMessageContainer searchText={search}>
              <ul>
                <li>
                  <span
                    className="link-text"
                    onClick={clearSearch}
                  >
                    {T.translate(`features.EmptyMessageContainer.clearLabel`)}
                  </span>
                  <span>{T.translate(`${PREFIX}.Content.EmptymessageContainer.suggestion1`)} </span>
                </li>
              </ul>
            </EmptyMessageContainer>
          </div>
        </div>
      </div>
    );
  }

  if (enableRouting) {
    return (
      <div className="gcs-buckets">
        {
          filteredData.map(file => {
            let lastModified = humanReadableDate(file['updated'], true);
            let size = convertBytesToHumanReadable(file['size'], HUMANREADABLESTORAGE_NODECIMAL, true);
            let type = file.directory ? T.translate(`${PREFIX}.Content.directory`) : file.type;

            if (file.type === 'UNKNOWN') {
              type = '--';
            }

            return (
              <ContainerElement
                className={classnames({'disabled': !file.directory && !file.wrangle})}
                to={`${pathname}?prefix=${getPrefix(file, prefix)}`}
                onClick={onClickHandler.bind(null, enableRouting, onWorkspaceCreate, file, prefix)}
              >
                <div className="row">
                  <div className="col-xs-3">
                    <IconSVG name={file.directory ? 'icon-folder-o' : 'icon-file-o'} />
                    {file.name}
                  </div>
                  <div className="col-xs-3">
                    {type}
                  </div>
                  <div className="col-xs-3">
                    {size}
                  </div>
                  <div className="col-xs-3">
                    {lastModified}
                  </div>
                </div>
              </ContainerElement>
            );
          })
        }
      </div>
    );
  }
  return (
    <div className="gcs-buckets">
      {
        filteredData.map(file => (
          <ContainerElement
            className={classnames({'disabled': !file.directory && !file.wrangle})}
            to={`${pathname}?prefix=${getPrefix(file, prefix)}`}
            onClick={onClickHandler.bind(null, enableRouting, onWorkspaceCreate, file, prefix)}
          >
            <div className="row">
              <div className="col-xs-12">
                <IconSVG name={file.directory ? 'icon-folder-o' : 'icon-file-o'} />
                {file.name}
              </div>
            </div>
          </ContainerElement>
        ))
      }
    </div>
  );
};
TableContents.propTypes = props;

const BrowserData = ({data, search, clearSearch, loading, prefix, enableRouting, onWorkspaceCreate}) => {
  if (loading) {
    return <LoadingSVGCentered />;
  }

  // FIXME: Possible? May be a proper empty message?
  if (!Object.keys(data).length) {
    return null;
  }

  return (
    <div>
      <div className="gcs-content-header">
        <TableHeader enableRouting={enableRouting} />
      </div>
      <div className="gcs-content-body">
        <TableContents
          search={search}
          clearSearch={clearSearch}
          data={data}
          prefix={prefix}
          enableRouting={enableRouting}
          onWorkspaceCreate={onWorkspaceCreate}
        />
      </div>
    </div>
  );
};

BrowserData.propTypes = props;

const mapStateToProps = (state, ownProps) => {
  let {enableRouting = true, onWorkspaceCreate = () => {}} = ownProps;
  return {
    data: state.gcs.activeBucketDetails,
    search: state.gcs.search,
    loading: state.gcs.loading,
    prefix: state.gcs.prefix,
    enableRouting,
    onWorkspaceCreate
  };
};

const mapDispatchToProps = () => {
  return {
    clearSearch: () => {
      setGCSSearch('');
    }
  };
};

const BrowserDataWrapper = connect(
  mapStateToProps,
  mapDispatchToProps
)(BrowserData);

export default BrowserDataWrapper;
