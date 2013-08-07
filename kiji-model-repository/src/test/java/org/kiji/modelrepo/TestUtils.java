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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.google.common.collect.Lists;

public final class TestUtils {

  /**
   * Dummy constructor.
   */
  private TestUtils() {
  }

  /**
   * Return a list of fake dependency "jars".
   *
   * @param numDeps is the number of fake dependencies to create.
   * @return a list of absolute locations to dependent jars.
   * @throws IOException if there is a problem creating the temporary jars.
   */
  public static List<File> getDependencies(int numDeps) throws IOException {
    return getDependencies(numDeps, null);
  }

  /**
   * Return a list of fake dependency "jars".
   *
   * @param numDeps is the number of fake dependencies to create.
   * @param parentDir is the directory in which to create this fake jar.
   * @return a list of absolute locations to dependent jars.
   * @throws IOException if there is a problem creating the temporary jars.
   */
  public static List<File> getDependencies(int numDeps, File parentDir) throws IOException {
    List<File> dependencies = Lists.newArrayList();
    for (int i = 0; i < numDeps; i++) {
      String dependencyName = "dependency-" + i;
      File temp = createFakeJar(dependencyName, parentDir);
      dependencies.add(temp);
    }
    return dependencies;
  }

  /**
   * Creates a fake jar in the root tmp directory and returns a handle to this file.
   *
   * @param fileName is the name of the fake jar to create.
   * @return a handle to the file object.
   * @throws IOException if there is a problem creating the fake jar.
   */
  public static File createFakeJar(String fileName) throws IOException {
    return createFakeJar(fileName, null);
  }

  /**
   * Creates a fake jar in the specified directory, returning a handle to this file.
   *
   * @param fileName is the name of the fake jar to create.
   * @param parentDir is the directory in which to create this fake jar.
   * @return a handle to the created file object.
   * @throws IOException if there is a problem creating this jar.
   */
  public static File createFakeJar(String fileName, File parentDir) throws IOException {
    File temp = null;
    if (parentDir == null) {
      temp = File.createTempFile(fileName, ".jar");
    } else {
      temp = File.createTempFile(fileName, ".jar", parentDir);
    }
    temp.deleteOnExit();

    // Write something to the file so that we can read it out to make sure
    // the jar creation itself didn't screw up the file.
    PrintWriter writer = new PrintWriter(temp);
    writer.println(fileName);
    writer.close();

    return temp;
  }
}
