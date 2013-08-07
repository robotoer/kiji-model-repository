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

import org.junit.Assert;
import org.junit.Test;

public class TestMavenDependencyResolver {

  @Test
  public void testShouldResolveDependenciesFromPom() throws Exception {
    File pomFile = new File("src/test/resources/pom.xml");
    Assert.assertTrue(pomFile.exists());
    MavenDependencyResolver resolver = new MavenDependencyResolver();
    List<File> resolvedDependencies = resolver.resolveDependencies(pomFile.getAbsolutePath());
    // This is a hard one to test because absolute paths are system dependent so let's check that
    // the dependencies exist as we think.
    Collections.sort(resolvedDependencies);
    Assert.assertEquals(4, resolvedDependencies.size());
    Assert.assertTrue(resolvedDependencies.get(0).getName().contains("commons-codec"));
    Assert.assertTrue(resolvedDependencies.get(1).getName().contains("commons-logging"));
    Assert.assertTrue(resolvedDependencies.get(2).getName().contains("httpclient"));
    Assert.assertTrue(resolvedDependencies.get(3).getName().contains("httpcore"));
  }
}
