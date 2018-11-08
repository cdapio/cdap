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
import * as React from 'react';
import NavLinkWrapper from 'components/NavLinkWrapper';
import { Theme } from 'services/ThemeHelper';
import { withContext } from 'components/Header/NamespaceLinkContext';
import classnames from 'classnames';
import T from 'i18n-react';

require('./AnalyticsLink.scss');

interface IAnalyticsLinkProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
  match: {
    isExact: boolean;
  };
}

class AnalyticsLink extends React.PureComponent<IAnalyticsLinkProps> {
  public render() {
    if (Theme.showAnalytics === false) {
      return null;
    }

    const featureName = Theme.featureNames.analytics;
    const { namespace, isNativeLink } = this.props.context;
    const analyticsUrl = `/ns/${namespace}/experiments`;
    return (
      <li
        id="navbar-analytics"
        className={classnames({
          active: this.isMMDSActive(),
        })}
      >
        <NavLinkWrapper
          isNativeLink={isNativeLink}
          to={isNativeLink ? `/cdap${analyticsUrl}` : analyticsUrl}
        >
          {featureName}
        </NavLinkWrapper>
      </li>
    );
  }

  private isMMDSActive = (location = window.location): boolean => {
    const match = this.props.match;
    if (match && match.isExact) {
      return true;
    }
    const { namespace } = this.props.context;
    const experimentsBasePath = `/cdap/ns/${namespace}/experiments`;
    return location.pathname.startsWith(experimentsBasePath);
  };
}

const AnalyticsLinkWithContext = withContext(AnalyticsLink);

export default AnalyticsLinkWithContext;
