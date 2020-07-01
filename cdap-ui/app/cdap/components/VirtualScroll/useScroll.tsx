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

import { useRef, useState, useLayoutEffect, MutableRefObject } from 'react';

export function useScroll<T extends HTMLElement = HTMLDivElement>(): [number, MutableRefObject<T>] {
  const [scrollTop, setScrollTop] = useState(0);
  const ref = useRef<T>();

  const onScroll = (e) =>
    requestAnimationFrame(() => {
      setScrollTop(e.target.scrollTop);
    });

  useLayoutEffect(() => {
    const scrollContainer = ref.current;

    if (scrollContainer === undefined) {
      return;
    }
    setScrollTop(scrollContainer.scrollTop);
    scrollContainer.addEventListener('scroll', onScroll);
    return () => scrollContainer.removeEventListener('scroll', onScroll);
  }, []);

  return [scrollTop, ref];
}
