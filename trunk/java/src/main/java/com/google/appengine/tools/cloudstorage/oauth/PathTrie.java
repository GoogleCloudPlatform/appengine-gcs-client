/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.cloudstorage.oauth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple trie for path elements.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
final class PathTrie {

  private Map<String, Map> root = new HashMap<>();

  PathTrie(Set<? extends Iterable<String>> paths) {
    for (Iterable<String> path : paths) {
      add(path);
    }
  }

  public void add(Iterable<String> path) {
    Map<String, Map> current = root;
    for (String pathElement : path) {
      Map<String, Map> child = current.get(pathElement);
      if (child == null) {
        child = new HashMap<>();
        current.put(pathElement, child);
      }
      current = child;
    }
  }

  public boolean contains(Iterable<String> path) {
    Map<String, Map> current = root;
    for (String pathElement : path) {
      Map<String, Map> child = current.get(pathElement);
      if (child == null) {
        return false;
      }
      current = child;
    }
    return true;
  }
}