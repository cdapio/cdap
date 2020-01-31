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

const RenderObjectAsTable = ({ obj }) => {
  return (
    <table className="table-borderless">
      <tbody>
        {Object.keys(obj).map((node, i) => {
          return (
            <tr key={i}>
              <td>{node}</td>
              <td>{obj[node]}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
};

RenderObjectAsTable.propTypes = {
  obj: PropTypes.object,
};

export default RenderObjectAsTable;
