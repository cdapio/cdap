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

const MissingFeatures: React.FC = () => {
  return (
    <div>
      <table className="table">
        <thead>
          <tr>
            <th>Issue Name</th>
            <th>Description</th>
            <th>Impact</th>
          </tr>
        </thead>

        <tbody>
          <tr>
            <td>Foreign key</td>
            <td>Foreign key is not supported in BigQuery</td>
            <td>The foreign key column will be dropped</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
};

export default MissingFeatures;
