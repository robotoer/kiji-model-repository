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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;

import org.kiji.modelrepo.MavenUtils;

/**
 * Resolves dependencies by using a Maven POM file. The dependency input is the
 * location to the pom file and the resulting dependencies will be resolved by obtaining
 * the runtime classpath of the project.
 *
 */
public class MavenDependencyResolver extends RawDependencyResolver {

  private Invoker mMavenInvoker = MavenUtils.getInvoker();

  @Override
  public List<File> resolveDependencies(String dependencyInput) throws IOException {

    Preconditions.checkNotNull(dependencyInput, "Input POM file can't be null.");

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
    BufferedReader reader = null;

    try {
      InvocationResult result = mMavenInvoker.execute(request);
      if (result.getExitCode() != 0) {
        throw new IOException(result.getExecutionException());
      }
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(tempClasspathFile),
          Charset.defaultCharset()));
      StringBuilder classPathLines = new StringBuilder();
      String line = reader.readLine();
      while (line != null) {
        classPathLines.append(line + ":");
        line = reader.readLine();
      }

      return super.resolveDependencies(classPathLines.toString());

    } catch (MavenInvocationException mie) {
      throw new IOException(mie);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
