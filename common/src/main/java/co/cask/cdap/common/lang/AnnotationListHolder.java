/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.annotation.ExposeClass;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Holds the list of annotations that can be used for loading classes
 * based on annotations by {@link co.cask.cdap.common.lang.jar.ExposeFilterClassLoader}
 */
public final class AnnotationListHolder {
  private static List<String> annotationList;

  private AnnotationListHolder() { }

  public static synchronized Iterable<String> getAnnotationList() throws IOException {
    if (annotationList == null) {
      annotationList = Lists.newArrayList();
      annotationList.add(ExposeClass.class.getName());
    }
    return annotationList;
  }
}
