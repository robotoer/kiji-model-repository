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

import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.Invoker;

/**
 * Contains utility methods around the use of the Maven invoker plugin to help downstream
 * clients.
 */
public final class MavenUtils {

  private static Invoker mInvoker;

  /** Dummy Contructor. **/
  private MavenUtils()  { }

  /**
   * Returns a singleton Maven invoker object.
   *
   * @return a singleton Maven invoker object.
   */
  public static Invoker getInvoker() {
    if (mInvoker == null) {
      mInvoker = new DefaultInvoker();
      if (mInvoker.getMavenHome() == null) {
        File possibleMavenHome = findMavenHome();
        if (possibleMavenHome != null) {
          mInvoker.setMavenHome(possibleMavenHome);
        }
      }
    }
    return mInvoker;
  }

  /**
   * Returns the first location (via the PATH variable) of the mvn executable or null
   * if no such location exists.
   *
   * @return the first location (via the PATH variable) of the mvn executable or null if no such
   *         location exists.
   */
  private static File findMavenHome() {
    final String pathEnv = System.getenv("PATH");
    if (pathEnv != null) {
      String[] pathParts = pathEnv.split(File.pathSeparator);
      for (String path : pathParts) {
        File possibleMvnPath = new File(path, "mvn");
        if (possibleMvnPath.exists()) {
          if (possibleMvnPath.getParentFile() != null) {
            return possibleMvnPath.getParentFile().getParentFile();
          } else {
            return possibleMvnPath.getParentFile();
          }
        }
      }
    }
    return null;
  }
}
