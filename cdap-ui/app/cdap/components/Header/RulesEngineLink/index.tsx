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
require('./RulesEngineLink.scss');

interface IRulesEngineLinkProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
  match: {
    isExact: boolean;
  };
}

class RulesEngineLink extends React.PureComponent<IRulesEngineLinkProps> {
  public render() {
    if (Theme.showRulesEngine === false) {
      return null;
    }
    const featureName = Theme.featureNames.rulesEngine;
    const { namespace, isNativeLink } = this.props.context;
    const rulesEngineUrl = `/ns/${namespace}/rulesengine`;
    return (
      <li
        id="navbar-rules-engine"
        className={classnames({
          active: this.isRulesEngineActive(),
        })}
      >
        <NavLinkWrapper
          isNativeLink={isNativeLink}
          to={isNativeLink ? `/cdap${rulesEngineUrl}` : rulesEngineUrl}
        >
          {featureName}
        </NavLinkWrapper>
      </li>
    );
  }

  private isRulesEngineActive = (location = window.location): boolean => {
    const match = this.props.match;
    if (match && match.isExact) {
      return true;
    }
    const { namespace } = this.props.context;
    const rulesenginepath = `/cdap/ns/${namespace}/rulesengine`;
    return location.pathname.startsWith(rulesenginepath);
  };
}

const RulesEngineLinkWithContext = withContext(RulesEngineLink);

export default RulesEngineLinkWithContext;
