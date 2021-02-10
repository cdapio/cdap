/*
 * Copyright © 2021 Cask Data, Inc.
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
import CommentRounded from '@material-ui/icons/CommentRounded';
import makeStyles from '@material-ui/core/styles/makeStyles';
import { Theme } from '@material-ui/core/styles/createMuiTheme';

const useStyles = makeStyles<Theme, ICommentIconProps>(() => {
  return {
    root: {
      fontSize: ({ size }) => {
        switch (size) {
          case 'regular':
            return '2rem';
          case 'small':
            return '1rem';
          case 'large':
            return '3rem';
          default:
            return '1rem';
        }
      },
    },
  };
});

type ICommentSize = 'small' | 'regular' | 'large';

interface ICommentIconProps {
  onClick?: (e: React.MouseEvent<SVGElement>) => void;
  size?: ICommentSize;
  id?: string;
}

function CommentIcon({ onClick, size = 'regular', id }: ICommentIconProps) {
  const classes = useStyles({ size });
  return <CommentRounded id={id} className={classes.root} onClick={onClick || undefined} />;
}
export default CommentIcon;
