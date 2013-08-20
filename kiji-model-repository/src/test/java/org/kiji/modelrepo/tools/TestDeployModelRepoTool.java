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

package org.kiji.modelrepo.tools;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.modelrepo.TestUtils;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolTest;
import org.kiji.schema.util.ProtocolVersion;

public class TestDeployModelRepoTool extends KijiToolTest {

  private Kiji mKiji = null;
  private File mTempDir = null;

  @Before
  public void setupModelRepo() throws Exception {
    mKiji = createTestKiji();
    mTempDir = Files.createTempDir();

    mTempDir.deleteOnExit();
    KijiModelRepository.install(mKiji, mTempDir.toURI());
  }

  private List<String> getBaselineArgs() {
    return Lists.newArrayList(new String[] {
        "--definition=src/test/resources/org/kiji/samplelifecycle/model_definition.json"
        , "--environment=src/test/resources/org/kiji/samplelifecycle/model_environment.json"
        , "--message=Uploading Artifact"
        , "--kiji=" + mKiji.getURI().toString()
        ,
    });
  }

  private static String makeDependencyString(List<File> inputDeps) {
    StringBuilder builder = new StringBuilder();
    for (File f : inputDeps) {
      builder.append(f.getAbsolutePath());
      builder.append(":");
    }
    return builder.toString();
  }

  // 1) Test deploying a new model not specifying a version to a blank table.
  @Test
  public void testShouldDeployNewModelToBlankTable() throws Exception {
    // 1) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("0.0.1"));
    Assert.assertTrue(lifeCycleRow.containsColumn("model", "location"));
    Assert.assertEquals("Uploading Artifact", lifeCycleRow.getMostRecentValue("model", "message")
        .toString());
    String relativeLocation = lifeCycleRow.getMostRecentValue("model", "location").toString();
    Assert.assertEquals("org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 2) Test deploying a new model specifying a version to a blank table (and message).
  @Test
  public void testShouldDeployNewModelWithVersionToBlankTable() throws Exception {
    // 1) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=0.0.1");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("0.0.1"));
    Assert.assertTrue(lifeCycleRow.containsColumn("model", "location"));
    Assert.assertEquals("Uploading Artifact", lifeCycleRow.getMostRecentValue("model", "message")
        .toString());
    String relativeLocation = lifeCycleRow.getMostRecentValue("model", "location").toString();
    Assert.assertEquals("org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 3) Test deploying a new model to a populated table not specifying the version
  @Test
  public void testShouldDeployNewModelToPopulatedTable() throws Exception {
    // 1) Populate the table with some stuff
    KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    KijiTableWriter writer = table.openTableWriter();
    EntityId eid = table.getEntityId("org.kiji.test.sample_model", "1.0.0");
    writer.put(eid, "model", "location", "stuff");
    writer.close();
    table.release();

    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("1.0.1"));
    Assert.assertTrue(lifeCycleRow.containsColumn("model", "location"));
    Assert.assertEquals("Uploading Artifact", lifeCycleRow.getMostRecentValue("model", "message")
        .toString());
    String relativeLocation = lifeCycleRow.getMostRecentValue("model", "location").toString();
    Assert.assertEquals("org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 4) Test deploying a new model to a populated table specifying the version
  @Test
  public void testShouldDeployNewModelWithVersionToPopulatedTable() throws Exception {
    // 1) Populate the table with some stuff
    KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    KijiTableWriter writer = table.openTableWriter();
    EntityId eid = table.getEntityId("org.kiji.test.sample_model", "1.0.0");
    writer.put(eid, "model", "location", "stuff");
    writer.close();
    table.release();

    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=1.0.1");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("1.0.1"));
    Assert.assertTrue(lifeCycleRow.containsColumn("model", "location"));
    Assert.assertEquals("Uploading Artifact", lifeCycleRow.getMostRecentValue("model", "message")
        .toString());
    String relativeLocation = lifeCycleRow.getMostRecentValue("model", "location").toString();
    Assert.assertEquals("org/kiji/test/sample_model/1.0.1/sample_model-1.0.1.war",
        relativeLocation);

    repo.close();
  }

  // 5) Test deploying an existing model to a populated table specifying the version to get a
  // conflict exception.
  @Test
  public void testFailToDeployWithVersionConflict() throws Exception {
    // 1) Populate the table with some stuff
    KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    KijiTableWriter writer = table.openTableWriter();
    EntityId eid = table.getEntityId("org.kiji.test.sample_model", "1.0.0");
    writer.put(eid, "model", "location", "stuff");
    writer.close();
    table.release();

    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=1.0.0");
    args.addAll(getBaselineArgs());

    try {
      runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
      Assert.fail("Deploy passed when it should have failed with a version conflict.");
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals("Error Version 1.0.0 exists.", iae.getMessage());
    }
  }

  // 6) Test deploying an artifact that doesn't exist?
  @Test
  public void testFailToDeployWhenArtifactDoesntExist() throws Exception {
    // 2) Setup the artifact
    List<File> dependencies = TestUtils.getDependencies(5);
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";

    String bogusFile = String.format("non-existant-artifact-%d.jar", System.currentTimeMillis());
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(bogusFile);
    args.add("--deps=" + makeDependencyString(dependencies));
    args.add("--deps-resolver=raw");
    args.add("--version=1.0.0");
    args.addAll(getBaselineArgs());

    try {
      runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
      Assert.fail("Deploy passed when it should have failed with a version conflict.");
    } catch (IllegalArgumentException iae) {
      Assert.assertEquals("Error: " + bogusFile + " does not exist", iae.getMessage());
    }
  }

  // 7) Test deploying a model using pom.xml to resolve deps.
  @Test
  public void testShouldDeployNewModelToBlankTableUsingPom() throws Exception {
    // 1) Setup the artifact
    File artifactJar = TestUtils.createFakeJar("artifact");
    String groupName = "org.kiji.test";
    String artifactName = "sample_model";
    List<String> args = Lists.newArrayList();
    args.add(groupName);
    args.add(artifactName);
    args.add(artifactJar.getAbsolutePath());
    args.add("--deps=src/test/resources/pom.xml");
    args.add("--deps-resolver=maven");
    args.addAll(getBaselineArgs());

    int status = runTool(new DeployModelRepoTool(), args.toArray(new String[0]));
    Assert.assertEquals(BaseTool.SUCCESS, status);

    String expectedLocation = "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war";
    // Check that the artifact was deployed
    File deployedFile = new File(mTempDir, expectedLocation);
    Assert.assertTrue(deployedFile.exists());

    // Check some attributes of the table.
    KijiModelRepository repo = KijiModelRepository.open(mKiji);

    KijiRowData lifeCycleRow = repo.getModelLifeCycle(groupName, artifactName,
        ProtocolVersion.parse("0.0.1"));
    Assert.assertTrue(lifeCycleRow.containsColumn("model", "location"));
    Assert.assertEquals("Uploading Artifact", lifeCycleRow.getMostRecentValue("model", "message")
        .toString());
    String relativeLocation = lifeCycleRow.getMostRecentValue("model", "location").toString();
    Assert.assertEquals("org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war",
        relativeLocation);
    repo.close();

    JarInputStream jarIs = new JarInputStream(new FileInputStream(deployedFile));
    JarEntry entry = jarIs.getNextJarEntry();
    int dependentJarsFound = 0;
    while (entry != null) {
      if (entry.getName().contains(".jar")) {
        dependentJarsFound++;
      }
      entry = jarIs.getNextJarEntry();
    }
    jarIs.close();
    Assert.assertEquals(5, dependentJarsFound);
  }
}
