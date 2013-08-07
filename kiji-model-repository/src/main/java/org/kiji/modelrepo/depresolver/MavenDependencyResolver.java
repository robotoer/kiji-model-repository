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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;

import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;

/**
 * Resolves dependencies by using a Maven POM file. The dependency input is the
 * location to the pom file and the resulting dependencies will be resolved by obtaining
 * the runtime classpath of the project.
 *
 */
public class MavenDependencyResolver extends RawDependencyResolver {

  private Invoker mMavenInvoker = new DefaultInvoker();

  /**
   * Constructs a new Maven based dependency resolver. If not set, will try to find the
   * location of the mvn executable via the PATH environment variable. If this is not
   * successful, an IllegalArgumentException will be thrown as this resolver won't work.
   */
  public MavenDependencyResolver() {
    // Let's try and find the mvn binary on the PATH so that we don't get an exception
    // by the invoker for not setting maven.home or M2_HOME variables.
    File mavenHome = findMavenHome();
    if (mavenHome != null) {
      mMavenInvoker.setMavenHome(mavenHome);
    }
  }

  /**
   * Returns the first location (via the PATH variable) of the mvn executable or null
   * if no such location exists.
   *
   * @return the first location (via the PATH variable) of the mvn executable or null if no such
   *         location exists.
   */
  private File findMavenHome() {
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

  @Override
  public List<File> resolveDependencies(String dependencyInput) throws IOException {
    // Here, dependencyInput is a file to the pom.xml and we'll use that to fetch
    // the classpath and pass that to the super method that will validate and construct
    // the final dependency list.
    File tempClasspathFile = File.createTempFile("classpath", ".txt");
    tempClasspathFile.deleteOnExit();

    InvocationRequest request = new DefaultInvocationRequest();
    request.setPomFile(new File(dependencyInput));
    request.setGoals(Lists.newArrayList("dependency:build-classpath"));
    Properties props = new Properties();
    props.setProperty("includeScope", "runtime");
    props.setProperty("silent", "true");
    props.setProperty("mdep.outputFile", tempClasspathFile.getAbsolutePath());
    request.setProperties(props);

    try {
      InvocationResult result = mMavenInvoker.execute(request);
      if (result.getExitCode() != 0) {
        throw new IOException(result.getExecutionException());
      }
      BufferedReader reader = new BufferedReader(new FileReader(tempClasspathFile));
      StringBuilder classPathLines = new StringBuilder();
      String line = reader.readLine();
      while (line != null) {
        classPathLines.append(line + ":");
        line = reader.readLine();
      }
      reader.close();
      return super.resolveDependencies(classPathLines.toString());

    } catch (MavenInvocationException mie) {
      throw new IOException(mie);
    }
  }
}