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

package org.kiji.modelrepo.depresolver;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Interface whose implementations produce a list of fully qualified dependencies given
 * an input. The input is understood by the underlying implementation and contains enough
 * information to help produce the list of output dependency files.
 *
 */
public interface DependencyResolver {

  /**
   * Returns a list of fully qualified dependencies given an input specification.
   *
   * @param dependencyInput is the dependency input specification.
   * @return a list of fully qualified dependencies.
   * @throws IOException if there is a problem resolving dependencies.
   */
  List<File> resolveDependencies(String dependencyInput) throws IOException;
}
