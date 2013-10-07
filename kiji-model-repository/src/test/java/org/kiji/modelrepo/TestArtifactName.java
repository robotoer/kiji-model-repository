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

public class TestArtifactName {

  @Test
  public void testShouldCorrectlyParseNameWithHyphen() throws Exception {
    String name = "org.kiji.my-model-1.0.0";
    ArtifactName artifact = new ArtifactName(name);
    Assert.assertEquals("org.kiji.my-model", artifact.getName());
    Assert.assertEquals("1.0.0", artifact.getVersion().toString());
  }

  @Test
  public void testShouldCorrectlyParseName() throws Exception {
    String name = "org.kiji.my_model-1.0.0";
    ArtifactName artifact = new ArtifactName(name);
    Assert.assertEquals("org.kiji.my_model", artifact.getName());
    Assert.assertEquals("1.0.0", artifact.getVersion().toString());
  }

  @Test
  public void testShouldFailWhenFirstCharacterIsNonWord() throws Exception {
    String name = "-org.kiji.my_model-1.0.0";
    try {
      new ArtifactName(name);
      Assert.fail("Artifact name parsing succeeded when it should have failed.");
    } catch (IllegalArgumentException iae) {
      Assert.assertTrue(iae.getMessage().contains("Artifact name must be of the form:"));
    }
  }

  @Test
  public void testShouldFailWhenLastCharacterIsNonWord() throws Exception {
    String name = "org.kiji.my_model.-1.0.0";
    try {
      new ArtifactName(name);
      Assert.fail("Artifact name parsing succeeded when it should have failed.");
    } catch (IllegalArgumentException iae) {
      Assert.assertTrue(iae.getMessage().contains("Artifact name must be of the form:"));
    }
  }

  @Test
  public void testShouldFailWithInvalidCharacterInName() throws Exception {
    String name = "org.kiji.my_$model-1.0.0";
    try {
      new ArtifactName(name);
      Assert.fail("Artifact name parsing succeeded when it should have failed.");
    } catch (IllegalArgumentException iae) {
      Assert.assertTrue(iae.getMessage().contains("Artifact name must be of the form:"));
    }
  }

  @Test
  public void testShouldParseWithNoVersion() throws Exception {
    String name = "org.kiji.my_model";
    ArtifactName artifact = new ArtifactName(name);
    Assert.assertFalse(artifact.isVersionSpecified());
  }

  @Test
  public void testShouldFailWithEmptyVersionSpecified() throws Exception {
    String name = "org.kiji.my_model-";
    try {
      new ArtifactName(name);
      Assert.fail("Artifact name parsing succeeded when it should have failed.");
    } catch (IllegalArgumentException npe) {
      Assert.assertEquals("verString may not be empty", npe.getMessage());
    }
  }

  @Test
  public void testShouldFailIfNotAtLeastOnePeriod() throws Exception {
    String name = "noperiodinname-1.0.0";
    try {
      new ArtifactName(name);
      Assert.fail("Artifact name parsing succeeded when it should have failed.");
    } catch (IllegalArgumentException iae) {
      Assert.assertTrue(iae.getMessage().contains("Artifact name must be of the form:"));
    }
  }
}
