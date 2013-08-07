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

package org.kiji.modelrepo.packager;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Defines how artifacts are to be packaged. The final deployable model artifact is defined
 * by the name of the output file and a list of dependencies.
 */
public interface Packager {
  /**
   * Generates a code artifact named outputFile that will contain the specified dependencies.
   * @param outputFile is the name of the artifact file.
   * @param dependencies are the dependencies to bundle in the final artifact.
   * @throws IOException if there is a problem creating the artifact file.
   */
  void generateArtifact(File outputFile, List<File> dependencies) throws IOException;
}
