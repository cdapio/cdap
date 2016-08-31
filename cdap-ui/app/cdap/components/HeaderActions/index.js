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
import React from 'react';

export default function HeaderActions() {
  return (
    <ul className="navbar-list pull-right">
      <div className="navbar-item">
        <span className="fa fa-search"></span>
      </div>
      <div className="navbar-item">
        <span className="fa fa-bolt"></span>
      </div>
      <div className="navbar-item">
        <span className="fa fa-plus-circle text-success"></span>
      </div>
      <div className="navbar-item">
        <span className="fa fa-cog"></span>
      </div>
      <div className="navbar-item namespace-dropdown dropdown">
        <span> Namespace </span>
      </div>
    </ul>
  );
}
