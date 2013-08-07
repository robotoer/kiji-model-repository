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
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Produces a list of dependencies given a string list of files and/or directories separated
 * by the ":" character. This follows close to the Java classpath syntax except that glob syntax
 * is not supported. In other words, "/path/to/dir/*" is not supported but instead simply use
 * "/path/to/dir/". <br/>
 * <br/>
 * <b>Note:</b> This will only add jar files as dependencies.
 */
public class RawDependencyResolver implements DependencyResolver {

  private static final FileFilter JAR_FILTER = new FileFilter() {
    @Override
    public boolean accept(File pathname) {
      return pathname.getName().endsWith(".jar");
    }
  };

  @Override
  public List<File> resolveDependencies(String dependencyInput) throws IOException {
    // input is assumed to be ":" separated and may contain files and directories. No
    // globs.
    Set<File> dependencySet = Sets.newHashSet();
    String[] individualPaths = dependencyInput.split(":");
    for (String path : individualPaths) {
      populateDependencies(new File(path), dependencySet);
    }
    return Lists.newArrayList(dependencySet);
  }

  /**
   * Recursively descend through the file structure finding jars to add to the output
   * dependency set. Stop recursing when the current depth reaches the maximum so that we don't
   * infinitely recurse in case we hit some symlink or other weird case.
   *
   * @param rootPath is the path to start descending.
   * @param outputDeps is the set of output dependencies to populate.
   */
  private void populateDependencies(File rootPath, Set<File> outputDeps) {
    // Since this can be called from a method whose input is a list of directories and files,
    // need to double check that this is a valid dependency here too.
    if (rootPath.isFile() && rootPath.exists() && JAR_FILTER.accept(rootPath)) {
      outputDeps.add(rootPath);
    } else if (rootPath.isDirectory()) {
      for (File f : rootPath.listFiles(JAR_FILTER)) {
        outputDeps.add(f);
      }
    }
  }
}
