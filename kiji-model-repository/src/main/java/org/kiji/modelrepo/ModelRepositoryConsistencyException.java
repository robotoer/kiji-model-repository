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

import org.kiji.schema.util.ProtocolVersion;

/**
 * Tracks consistency issues in the model repository.
 */
public class ModelRepositoryConsistencyException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Construct model repository consistency exception with artifact name, version, and
   * custom message.
   *
   * @param artifactName string name of artifact.
   * @param artifactVersion protocol version of the artifact
   * @param message custom message
   */
  public ModelRepositoryConsistencyException(
      final String artifactName,
      final ProtocolVersion artifactVersion,
      final String message) {
    super(String.format("%s-%s: %s",
        artifactName,
        artifactVersion.toCanonicalString(),
        message));
  }

  /**
   * Construct model repository consistency exception with custom message.
   *
   * @param message custom message
   */
  public ModelRepositoryConsistencyException(final String message) {
    super(message);
  }

}
