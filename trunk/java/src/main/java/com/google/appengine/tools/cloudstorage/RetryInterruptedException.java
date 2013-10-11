/*
 * Copyright 2013 Google Inc. All Rights Reserved.
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


/**
 * Thrown when a RetryHelper failed to complete its work due to interruption.
 * Throwing this exception also sets the thread interrupt flag.
 */
public final class RetryInterruptedException extends RetryHelperException {

  private static final long serialVersionUID = 1678966737697204885L;

  RetryInterruptedException() {
  }

  /**
   * Sets the caller thread interrupt flag and throws {@code RetryInteruptedException}.
   */
  static void propagate() throws RetryInterruptedException {
    Thread.currentThread().interrupt();
    throw new RetryInterruptedException();
  }
}