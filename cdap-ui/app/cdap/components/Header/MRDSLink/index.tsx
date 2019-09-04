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
import {withContext} from 'components/Header/NamespaceLinkContext';
import classnames from 'classnames';
import { Theme } from 'services/ThemeHelper';
require('./MRDSLink.scss');

interface IMRDSLinkProps {
  context: {
    namespace: string;
  };
}

class MRDSLink extends React.PureComponent<IMRDSLinkProps> {
  public render() {
    if (Theme.showMRDS === false) {
      return null;
    }
    const headerName = "Models";
    const { namespace } = this.props.context;

    return (
      <li
        id="navbar-mrds"
        className={classnames({
          active: this.isMRDSActive(),
        })}
      onClick = {this.navigateToMRDS.bind(this, namespace)}>
      {headerName}
      </li>
    );
  }

  protected navigateToMRDS = (namespace) => {
    const mrdsURL = `/ns/${namespace}/mrds`;
    const mrdsPath = `/cdap${mrdsURL}`;
    window.location.href = mrdsPath;
  }

  protected isMRDSActive = (location = window.location): boolean => {
    return (location.pathname.indexOf("mrds") >= 0);
  }
}

const MRDSLinkWithContext = withContext(MRDSLink);

export default MRDSLinkWithContext;
