/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.modelrepo;

import org.apache.maven.shared.invoker.InvocationOutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Define an invocation output handler that writes output to a configurable logger
 * instead of System.out which is the default for the Maven invoker API.
 *
 */
public class LoggerOutputHandler implements InvocationOutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LoggerOutputHandler.class);

  @Override
  public void consumeLine(String line) {
    LOG.info(line);
  }

}
