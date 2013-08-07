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
import java.util.Collections;
import java.util.List;

import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Test;

import org.kiji.modelrepo.TestUtils;

public class TestRawDependencyResolver {

  @Test
  public void testShouldResolveDirectoryOfDependencies() throws Exception {
    File tempLibDir = Files.createTempDir();
    tempLibDir.deleteOnExit();

    // Create a temporary directory of libs
    List<File> expectedDeps = TestUtils.getDependencies(5, tempLibDir);
    DependencyResolver resolver = new RawDependencyResolver();
    List<File> resolvedDeps = resolver.resolveDependencies(tempLibDir.getAbsolutePath());

    // Make sure that the expected dependencies match the resolved ones.
    Collections.sort(expectedDeps);
    Collections.sort(resolvedDeps);

    Assert.assertEquals(expectedDeps, resolvedDeps);
  }

  @Test
  public void testShouldResolveDirectoriesAndFiles() throws Exception {
    File tempLibDir = Files.createTempDir();
    tempLibDir.deleteOnExit();

    // Create a directory of expected dependencies
    List<File> expectedDeps = TestUtils.getDependencies(5, tempLibDir);
    // Create a bunch of dependencies in the root tmp directory.
    List<File> expectedDepsInTmp = TestUtils.getDependencies(5);

    expectedDeps.addAll(expectedDepsInTmp);

    // Add each file in the root temp dir to the dependencies list so that we have a mix of
    // files and a directory.
    StringBuilder fileDependencies = new StringBuilder(expectedDepsInTmp.get(0).getAbsolutePath());
    for (int i = 1; i < expectedDepsInTmp.size(); i++) {
      fileDependencies.append(":" + expectedDepsInTmp.get(i));
    }

    DependencyResolver resolver = new RawDependencyResolver();
    List<File> resolvedDeps = resolver.resolveDependencies(tempLibDir.getAbsolutePath()
        + ":" + fileDependencies.toString());
    Collections.sort(resolvedDeps);
    Collections.sort(expectedDeps);

    Assert.assertEquals(expectedDeps, resolvedDeps);
  }
}
