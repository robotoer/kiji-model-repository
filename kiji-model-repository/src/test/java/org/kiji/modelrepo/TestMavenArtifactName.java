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

import org.junit.Assert;
import org.junit.Test;

import org.kiji.modelrepo.uploader.MavenArtifactName;

public class TestMavenArtifactName {

  @Test
  public void testShouldCorrectlyParseNameWithHyphen() throws Exception {
    String name = "org.kiji.my-model-1.0.0";
    ArtifactName artifact = new ArtifactName(name);
    MavenArtifactName mavenArtifact = new MavenArtifactName(artifact);

    Assert.assertEquals("org.kiji", mavenArtifact.getGroupName());
    Assert.assertEquals("my-model", mavenArtifact.getArtifactName());
    Assert.assertEquals("1.0.0", mavenArtifact.getVersion().toString());
  }

  @Test
  public void testShouldCorrectlyParseName() throws Exception {
    String name = "org.kiji.my_model-1.0.0";
    ArtifactName artifact = new ArtifactName(name);
    MavenArtifactName mavenArtifact = new MavenArtifactName(artifact);

    Assert.assertEquals("org.kiji", mavenArtifact.getGroupName());
    Assert.assertEquals("my_model", mavenArtifact.getArtifactName());
    Assert.assertEquals("1.0.0", artifact.getVersion().toString());
  }

  @Test
  public void testShouldFailIfNoVersionSpecified() throws Exception {
    String name = "org.kiji.my_model";
    ArtifactName artifact = new ArtifactName(name);
    try {
      new MavenArtifactName(artifact);
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals("Artifact version must be specified.", iae.getMessage());
    }
  }
}
