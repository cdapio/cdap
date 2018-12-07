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
require('./PipelinesLink.scss');

interface IPipelinesLinkProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
  match: {
    isExact: boolean;
  };
}

class PipelinesLink extends React.PureComponent<IPipelinesLinkProps> {
  private isPipelinesActive = (location = window.location): boolean => {
    const match = this.props.match;
    if (match && match.isExact) {
      return true;
    }

    const { namespace } = this.props.context;
    const pipelinesLink = '/pipelines';
    const pipelinesBasePath = `/cdap/ns/${namespace}${pipelinesLink}`;

    if (
      location.pathname.startsWith(pipelinesLink) ||
      location.pathname.startsWith(pipelinesBasePath)
    ) {
      return true;
    }

    return false;
  };

  public render() {
    if (Theme.showPipelines === false) {
      return null;
    }

    const featureName = Theme.featureNames.pipelines;
    const { namespace, isNativeLink } = this.props.context;
    const pipelinesListUrl = `/ns/${namespace}/pipelines`;

    return (
      <li
        id="navbar-pipelines"
        className={classnames({
          active: this.isPipelinesActive(),
        })}
      >
        <NavLinkWrapper
          isNativeLink={isNativeLink}
          to={isNativeLink ? `/cdap${pipelinesListUrl}` : pipelinesListUrl}
        >
          {featureName}
        </NavLinkWrapper>
      </li>
    );
  }
}

const PipelinesLinkWithContext = withContext(PipelinesLink);

export default PipelinesLinkWithContext;
