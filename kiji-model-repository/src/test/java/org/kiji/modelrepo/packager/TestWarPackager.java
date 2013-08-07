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

import static org.kiji.modelrepo.TestUtils.createFakeJar;
import static org.kiji.modelrepo.TestUtils.getDependencies;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

public class TestWarPackager {

  @Test
  public void testShouldCreateProperWarFileWithDependencies() throws Exception {
    Packager warPackager = new WarPackager();
    File artifactJar = createFakeJar("artifact");
    List<File> dependentJars = getDependencies(2);
    dependentJars.add(artifactJar);
    File finalWar = File.createTempFile("packaged_artifact", ".war");
    warPackager.generateArtifact(finalWar, dependentJars);
    Assert.assertTrue(isValidWar(finalWar, dependentJars));
  }

  @Test
  public void testShouldCreateProperWarFileWithNoDependencies() throws Exception {
    Packager warPackager = new WarPackager();
    File artifactJar = createFakeJar("artifact");
    List<File> dependentJars = Lists.newArrayList();
    dependentJars.add(artifactJar);
    File finalWar = File.createTempFile("packaged_artifact", ".war");
    warPackager.generateArtifact(finalWar, dependentJars);
    Assert.assertTrue(isValidWar(finalWar, dependentJars));
  }

  /**
   * Determines if the given war file is valid with respect to the dependencies expected.
   * Will open up the war file and inspect each "jar" in the war to make sure that it has
   * the properly stored dependencies.
   *
   * @param warFile is the war file to validate.
   * @param expectedDependencies is the list of expected dependencies to be stored in WEB-INF/lib
   * @return whether or not this war file is valid.
   * @throws IOException if there is a problem reading the file or any contents.
   */
  private boolean isValidWar(File warFile, List<File> expectedDependencies)
      throws IOException {
    JarInputStream is = new JarInputStream(new FileInputStream(warFile));
    JarEntry nextEntry = is.getNextJarEntry();
    int numEntries = 0;
    boolean containsLibDir = false;
    boolean filesValid = true;
    while (nextEntry != null) {
      if ("WEB-INF/lib/".equalsIgnoreCase(nextEntry.getName())) {
        containsLibDir = true;
      } else {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String fileContents = reader.readLine();
        // The jar has written in it, the name of the file to keep validation simple. This
        // is simply testing that the file was stored properly in the war file and can be
        // read back out as expected.
        filesValid = filesValid && nextEntry.getName().contains(fileContents);
        numEntries++;
      }
      nextEntry = is.getNextJarEntry();
    }
    is.close();
    return containsLibDir && (numEntries == expectedDependencies.size()) && filesValid;
  }


}
