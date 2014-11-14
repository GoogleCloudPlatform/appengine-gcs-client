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

package com.google.appengine.tools.cloudstorage;

import com.google.common.collect.AbstractIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Contains the result of a listing request.
 * Iterating over the items may throw {@code RuntimeException}
 * if a GCS request is needed to fetch more data.
 */
public final class ListResult extends AbstractIterator<ListItem> {

  private final Callable<Iterator<ListItem>> batcher;
  private Iterator<ListItem> currentBatch;

  public ListResult(Callable<Iterator<ListItem>> batcher) throws IOException {
    this.batcher = batcher;
    try {
      currentBatch = batcher.call();
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  @Override
  protected ListItem computeNext() {
    if (currentBatch == null) {
      return endOfData();
    }
    if (currentBatch.hasNext()) {
      return currentBatch.next();
    }
    try {
      currentBatch = batcher.call();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return computeNext();
  }
}
