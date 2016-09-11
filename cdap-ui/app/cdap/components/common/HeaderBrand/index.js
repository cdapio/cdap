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
var classNames = require('classnames');

export default function HeaderBrand ({title, icon, onClickHandler}) {
  return (
    <div className="brand-header">
      <a className="navbar-brand"
         onClick={onClickHandler}>
        <span className={classNames('fa', icon)}></span>
      </a>
      <a href="/" className="menu-item product-title">
        {title}
      </a>
    </div>
  );
}

HeaderBrand.propTypes = {
  title: PropTypes.string,
  icon: PropTypes.string,
  onClickHandler: PropTypes.func
};
