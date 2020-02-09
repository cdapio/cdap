/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import { connect } from 'react-redux';
import IconSVG from 'components/IconSVG';
import { setSearch } from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
import T from 'i18n-react';

interface ISearchBoxProps {
  value: string;
  onChange: () => void;
}
const PREFIX = 'features.PipelineList';

const SearchBoxView: React.SFC<ISearchBoxProps> = ({ value, onChange }) => {
  return (
    <div className="search-box">
      <div className="input-group">
        <div className="input-group-prepend">
          <div className="input-group-text">
            <IconSVG name="icon-search" />
          </div>
        </div>
        <input
          type="text"
          className="form-control"
          placeholder={T.translate(`${PREFIX}.DeployedPipelineView.searchPlaceholder`).toString()}
          value={value}
          onChange={onChange}
        />
      </div>
    </div>
  );
};

const mapStateToProps = (state) => {
  return {
    value: state.deployed.search,
  };
};

const mapDispatch = () => {
  return {
    onChange: (e) => {
      setSearch(e.target.value);
    },
  };
};

const SearchBox = connect(
  mapStateToProps,
  mapDispatch
)(SearchBoxView);

export default SearchBox;
