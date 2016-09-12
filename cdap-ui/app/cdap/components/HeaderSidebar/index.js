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
import AbsLinkTo from '../AbsLinkTo';
import T from 'i18n-react';

export default function HeaderSidebar ({onClickHandlerNoOp}) {
  const getContext = (extension) => {
    switch(extension) {
      case 'hydrator':
        return {
          uiApp: 'cask-hydrator'
        };
      case 'tracker':
        return {
          uiApp: 'cask-tracker'
        };
      default:
        return {};
    }
  };
  return (
    <div className="sidebar" onClick={onClickHandlerNoOp}>
      <a href="/"
         className="brand sidebar-item top">
        <div className="brand-icon text-center cdap">
          <span className="icon-fist"></span>
        </div>
        {/* This will change once we introduce navbar for hydraotr & tracker in react*/}
        <div className="product-name">
          <T.span text={{ key: "commons.cdap" }} />
        </div>
      </a>
      <h5>
        <T.span text={{ key: "features.Navbar.Sidebar.extension" }} />
      </h5>
      <AbsLinkTo
        context={getContext('hydrator')}
        className="brand sidebar-item"
      >
        <div className="brand-icon text-center hydrator">
          <span className="icon-hydrator"></span>
        </div>

        <div className="product-name">
          <T.span text={{ key: "commons.hydrator" }} />
        </div>
      </AbsLinkTo>
      <AbsLinkTo
        context={getContext('tracker')}
        className="brand sidebar-item"
      >
        <div className="brand-icon text-center tracker">
          <span className="icon-tracker"></span>
        </div>

        <div className="product-name">
          <T.span text={{ key: "commons.tracker" }} />
        </div>
      </AbsLinkTo>
    </div>
  );
}
HeaderSidebar.propTypes = {
  onClickHandlerNoOp: PropTypes.func
};
