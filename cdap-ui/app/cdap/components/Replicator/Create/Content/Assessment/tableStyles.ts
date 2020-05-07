/*
 * Copyright © 2020 Cask Data, Inc.
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

import { StyleRules } from '@material-ui/core/styles/withStyles';

export function getSchemaTableStyles(theme, gridTemplateColumns): StyleRules {
  return {
    root: {
      height: '100%',
    },
    text: {
      color: theme.palette.grey[100],
      marginBottom: '10px',
    },
    gridWrapper: {
      height: '100%',
      '& .grid.grid-container.grid-compact': {
        maxHeight: '300px',

        '& .grid-header > .grid-row': {
          alignItems: 'end',
          lineHeight: 1.2,
        },

        '& .grid-row': {
          gridTemplateColumns,

          '& > div:not(:first-child)': {
            textAlign: 'right',
          },

          '& > div:last-child': {
            paddingRight: '25px',
          },

          '&:hover $mappingButton': {
            color: theme.palette.blue[200],
          },
        },
      },
    },
    mappingButton: {
      color: theme.palette.grey[200],
      cursor: 'pointer',
      '&:hover': {
        textDecoration: 'underline',
      },
    },
  };
}

export function getGenericIssuesTableStyles(theme): StyleRules {
  return {
    root: {
      height: '100%',
    },
    text: {
      color: theme.palette.grey[100],
      marginBottom: '10px',
    },
    gridWrapper: {
      height: 'calc(100% - 30px)',

      '& .grid.grid-container.grid-compact': {
        maxHeight: '100%',

        '& .grid-row': {
          gridTemplateColumns: '1fr 2fr 1fr 1fr',
          gridColumnGap: '50px',
        },
      },
    },
  };
}
