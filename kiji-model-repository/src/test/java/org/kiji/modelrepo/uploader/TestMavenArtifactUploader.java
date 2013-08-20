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

package org.kiji.modelrepo.uploader;

import java.io.File;

import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Test;

import org.kiji.modelrepo.TestUtils;
import org.kiji.schema.util.ProtocolVersion;

public class TestMavenArtifactUploader {

  @Test
  public void testShouldUploadArtifactToRightLocation() throws Exception {
    // Let's create a fake artifact and then upload it to some dummy temp directory
    // on disk.
    File tempDeployLocation = Files.createTempDir();
    tempDeployLocation.deleteOnExit();

    File tempArtifact = TestUtils.createFakeJar("my_final_artifact", ".war", null);

    ArtifactUploader uploader = new MavenArtifactUploader();
    String expectedRelativeLocation =
        "org/kiji/models/checkout_model/1.0.0/checkout_model-1.0.0.war";
    String actualLocation = uploader.uploadArtifact("org.kiji.models", "checkout_model",
        ProtocolVersion.parse("1.0.0"), tempDeployLocation.toURI(), tempArtifact);

    Assert.assertEquals(expectedRelativeLocation, actualLocation);

    File expectedArtifactLocation = new File(tempDeployLocation,
       expectedRelativeLocation);
    Assert.assertTrue(expectedArtifactLocation.exists());
  }

  @Test
  public void testShouldUploadArtifactWithSimpleGroupName() throws Exception {
    // Let's create a fake artifact and then upload it to some dummy temp directory
    // on disk.
    File tempDeployLocation = Files.createTempDir();
    tempDeployLocation.deleteOnExit();

    File tempArtifact = TestUtils.createFakeJar("my_final_artifact", ".war", null);

    ArtifactUploader uploader = new MavenArtifactUploader();
    uploader.uploadArtifact("myModels", "checkout_model", ProtocolVersion.parse("1.0.0"),
        tempDeployLocation.toURI(), tempArtifact);

    File expectedArtifactLocation = new File(tempDeployLocation,
        "myModels/checkout_model/1.0.0/checkout_model-1.0.0.war");
    Assert.assertTrue(expectedArtifactLocation.exists());
  }

  @Test
  public void testShouldUploadArtifactWithSimpleVersion() throws Exception {
    // Let's create a fake artifact and then upload it to some dummy temp directory
    // on disk.
    File tempDeployLocation = Files.createTempDir();
    tempDeployLocation.deleteOnExit();

    File tempArtifact = TestUtils.createFakeJar("my_final_artifact", ".war", null);

    ArtifactUploader uploader = new MavenArtifactUploader();
    uploader.uploadArtifact("myModels", "checkout_model", ProtocolVersion.parse("1"),
        tempDeployLocation.toURI(), tempArtifact);

    File expectedArtifactLocation = new File(tempDeployLocation,
        "myModels/checkout_model/1.0.0/checkout_model-1.0.0.war");
    Assert.assertTrue(expectedArtifactLocation.exists());
  }
}
