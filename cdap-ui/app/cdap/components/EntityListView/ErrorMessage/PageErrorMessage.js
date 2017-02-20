/*
 * Copyright © 2016 Cask Data, Inc.
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
import React, {PropTypes} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import Link from 'react-router/Link';
import T from 'i18n-react';

require('./PageErrorMessage.scss');

export default function PageErrorMessage ({pageNum, query}) {
  let selectedNamespace = NamespaceStore.getState().selectedNamespace;
  let page1Url = `/ns/${selectedNamespace}?page=1`;
  page1Url = query ? `${page1Url}&q=${query}` : page1Url;
  return (
    <div className="page-error-message empty-message">
      <h2>
        <span className="fa fa-exclamation-triangle"></span>
        {T.translate('features.EntityListView.PageErrorMessage.errorMessage', {pageNum})}
      </h2>
      <div>
        <span>
          {T.translate('features.EntityListView.PageErrorMessage.suggestionMessage1')}
          <Link to={page1Url}>
            {T.translate('features.EntityListView.PageErrorMessage.suggestionMessage2')}
          </Link>
        </span>
      </div>
    </div>
  );
}
PageErrorMessage.propTypes = {
  pageNum: PropTypes.number,
  query: PropTypes.string,
};
