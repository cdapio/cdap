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

export default {
  direction: 'both',
  'start-ts': '1558571821',
  'end-ts': '1559176621',
  entityId: {
    namespace: 'default',
    dataset: 'Employee_Data',
  },
  fields: [
    'id',
    'name',
    'street_number',
    'street',
    'apt',
    'city',
    'zip',
    'ssn',
    'level',
    'dept_id',
    'designation',
    'joining_date',
    'area_code',
    'comp_2017',
    'comp_2018',
    'comp_2019',
  ],
  incoming: [
    {
      entityId: {
        namespace: 'default',
        dataset: 'Person_Data',
      },
      relations: [
        {
          source: 'id',
          destination: 'id',
        },
        {
          source: 'first_name',
          destination: 'name',
        },
        {
          source: 'last_name',
          destination: 'street_number',
        },
        {
          source: 'address',
          destination: 'city',
        },
      ],
    },
    {
      entityId: {
        namespace: 'default',
        dataset: 'HR_Data',
      },
      relations: [
        {
          source: 'id',
          destination: 'id',
        },
        {
          source: 'level',
          destination: 'level',
        },
        {
          source: 'designation',
          destination: 'comp_2017',
        },
        {
          source: 'date',
          destination: 'designation',
        },
        {
          source: 'date',
          destination: 'comp_2017',
        },
      ],
    },
    {
      entityId: {
        namespace: 'default',
        dataset: 'Skills_Data',
      },
      relations: [
        {
          source: 'id',
          destination: 'id',
        },
        {
          source: 'technical',
          destination: 'joining_date',
        },
        {
          source: 'ops',
          destination: 'comp_2017',
        },
      ],
    },
    {
      entityId: {
        namespace: 'default',
        dataset: 'Comp_Data',
      },
      relations: [
        {
          source: 'id',
          destination: 'id',
        },
        {
          source: 'first',
          destination: 'name',
        },
        {
          source: 'last',
          destination: 'name',
        },
        {
          source: 'start_date',
          destination: 'joining_date',
        },
      ],
    },
    {
      entityId: {
        namespace: 'default',
        dataset: 'Misc_Data',
      },
      relations: [
        {
          source: 'vacation',
          destination: 'city',
        },
      ],
    },
  ],
  outgoing: [
    {
      entityId: {
        namespace: 'default',
        dataset: 'Performance',
      },
      relations: [
        {
          source: 'id',
          destination: 'id',
        },
      ],
    },
    {
      entityId: {
        namespace: 'default',
        dataset: 'Promotion',
      },
      relations: [
        {
          source: 'id',
          destination: 'id',
        },
        {
          source: 'dept_id',
          destination: 'id',
        },
        {
          source: 'level',
          destination: 'new_level',
        },
        {
          source: 'designation',
          destination: 'new_designation',
        },
        {
          source: 'comp_2017',
          destination: '2017_comp',
        },
      ],
    },
  ],
};
