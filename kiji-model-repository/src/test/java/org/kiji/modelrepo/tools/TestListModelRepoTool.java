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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.google.common.io.Files;

import org.junit.Before;
import org.junit.Test;

import org.kiji.express.avro.AvroModelDefinition;
import org.kiji.express.avro.AvroModelEnvironment;
import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolTest;
import org.kiji.schema.util.FromJson;

public class TestListModelRepoTool extends KijiToolTest {

  private Kiji mKiji = null;
  private File mTempDir = null;

  @Before
  public void setupModelRepo() throws Exception {
    mKiji = createTestKiji();
    mTempDir = Files.createTempDir();
    mTempDir.deleteOnExit();
    KijiModelRepository.install(mKiji, mTempDir.toURI());
    // Manually, set up model repository table.
    final KijiTable table = mKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    final KijiTableWriter writer = table.openTableWriter();
    final EntityId linRegScore = table.getEntityId("org.kiji.fake.linregscore", "1.0.0");
    final EntityId linRegScoreNew = table.getEntityId("org.kiji.fake.linregscore", "1.1.0");
    final EntityId recommend = table.getEntityId("org.kiji.fake.recommend", "1.0.0");
    final EntityId fraudScore = table.getEntityId("org.kiji.fake.fraudscore", "1.0.0");
    final EntityId incompleteModel = table.getEntityId("org.kiji.fake.incomplete", "1.0.0");

    // Write locations
    writer.put(linRegScore, "model", "location", 1L, "nonexistent.jar");
    writer.put(linRegScoreNew, "model", "location", 2L, "nonexistent.jar");
    writer.put(recommend, "model", "location", 3L, "nonexistent.jar");
    writer.put(fraudScore, "model", "location", 4L, "nonexistent.jar");
    writer.put(incompleteModel, "model", "location", 4L, "nonexistent.jar");

    // Read fake test definition and write to all the entries.
    final File definitionFile =
        new File("src/test/resources/org/kiji/samplelifecycle/model_definition.json");
    final BufferedReader definitionReader = new BufferedReader(new FileReader(definitionFile));
    String line;
    String definitionJson = "";
    while ((line = definitionReader.readLine()) != null) {
      definitionJson += line;
    }
    definitionReader.close();
    final AvroModelDefinition modelDefinition = (AvroModelDefinition)
        FromJson.fromJsonString(definitionJson, AvroModelDefinition.SCHEMA$);
    writer.put(linRegScore, "model", "definition", 1L, modelDefinition);
    writer.put(linRegScoreNew, "model", "definition", 2L, modelDefinition);
    writer.put(recommend, "model", "definition", 3L, modelDefinition);
    writer.put(fraudScore, "model", "definition", 4L, modelDefinition);
    // Give incomplete model a definition since every model must have a definition.
    writer.put(incompleteModel, "model", "definition", 5L, modelDefinition);

    // Read fake environment test definition and write to all the entries
    final File environmentFile =
        new File("src/test/resources/org/kiji/samplelifecycle/model_environment.json");
    final BufferedReader environmentReader = new BufferedReader(new FileReader(environmentFile));
    String environmentJson = "";
    while ((line = environmentReader.readLine()) != null) {
      environmentJson += line;
    }
    environmentReader.close();
    final AvroModelEnvironment modelEnvironment = (AvroModelEnvironment)
        FromJson.fromJsonString(environmentJson, AvroModelEnvironment.SCHEMA$);
    writer.put(linRegScore, "model", "environment", 1L, modelEnvironment);
    writer.put(linRegScoreNew, "model", "environment", 2L, modelEnvironment);
    writer.put(recommend, "model", "environment", 3L, modelEnvironment);
    writer.put(fraudScore, "model", "environment", 4L, modelEnvironment);

    // Write a few messages and production_ready flags.
    writer.put(linRegScore, "model", "message", 1L, "Website uses linear regression scorer.");
    writer.put(linRegScore, "model", "production_ready", 1L, true);
    writer.put(linRegScore, "model", "message", 2L, "Turned off old linear regression scorer.");
    writer.put(linRegScore, "model", "production_ready", 2L, false);
    writer.put(linRegScoreNew, "model", "message", 2L, "Linear regression scorer was updated.");
    writer.put(linRegScoreNew, "model", "production_ready", 2L, true);
    writer.put(linRegScoreNew, "model", "message", 3L, "Linear regression is outdated");
    writer.put(linRegScoreNew, "model", "production_ready", 3L, false);
    writer.put(recommend, "model", "message", 3L, "Brand new recommendation engine online");
    writer.put(recommend, "model", "production_ready", 3L, true);
    writer.put(fraudScore, "model", "message", 4L, "FraudScore implemented.");
    writer.put(fraudScore, "model", "production_ready", 4L, true);
    writer.put(fraudScore, "model", "message", 4000L, "FraudScore functioned during cyberattack.");
    writer.put(fraudScore, "model", "message", 8000L, "FraudScore failed to detect new attacks.");
    writer.put(incompleteModel, "model", "message", 5L, "Should never be implemented.");
    writer.put(incompleteModel, "model", "production_ready", 5L, false);
    writer.put(incompleteModel, "model", "production_ready", 6L, true);
    writer.put(incompleteModel, "model", "production_ready", 7L, false);
    writer.put(incompleteModel, "model", "message", 8L, "Random comment.");
    writer.put(incompleteModel, "model", "message", 9L, "Random comment.");
    writer.put(incompleteModel, "model", "production_ready", 10L, true);
    writer.put(incompleteModel, "model", "production_ready", 11L, false);
    writer.put(incompleteModel, "model", "message", 11L, "Random comment.");
    writer.put(incompleteModel, "model", "message", 12L, "Random comment.");

    // Close table.
    writer.close();
    table.release();
  }

  @Test
  public void testShouldListOnlyModelEntryNamesAndVersions() throws Exception {
    // Run list model repository.
    final int status = runTool(new ListModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(), });
    assertEquals(BaseTool.SUCCESS, status);
    // Check 19 lines corresponding to 4 lines per entry * 4 entries + 3 lines for last entry.
    assertEquals(4 * 4 + 3, mToolOutputLines.length);
    // Even includes the "incomplete" artifact which had only a model definition and location.
    assertTrue(mToolOutputStr.contains("incomplete"));
  }

  @Test
  public void testShouldListEveryField() throws Exception {
    // Run list model repository.
    final int status = runTool(new ListModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(),
            "--definition", "--environment", "--location", "--message", "--production-ready", });
    assertEquals(BaseTool.SUCCESS, status);
    // 8 lines for 3 models, 7 lines for last model and 7 lines for incomplete model
    assertEquals(9 * 3 + 8 + 8, mToolOutputLines.length);
    // Even includes the "incomplete" artifact which had only a model definition and location.
    assertTrue(mToolOutputStr.contains("incomplete"));
    // Includes all the field names appropriate number of times.
    assertEquals(6, mToolOutputStr.split("definition:").length);
    assertEquals(5, mToolOutputStr.split("environment:").length);
    assertEquals(6, mToolOutputStr.split("location:").length);
    assertEquals(6, mToolOutputStr.split("message ").length);
    assertEquals(6, mToolOutputStr.split("production_ready ").length);
  }

  @Test
  public void testShouldListMessagesAndProductionReadyFlagsSortedByTimestamp() throws Exception {
    // Run list model repository.
    final int status = runTool(new ListModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(),
            "--artifact=incomplete", "--max-versions=all", "--message", "--production-ready", });
    assertEquals(BaseTool.SUCCESS, status);
    final String[] timestampPrefixedLines = mToolOutputStr.split("\\[");
    long currTimestamp = 0;
    for (int i = 1; i < timestampPrefixedLines.length; i++) {
      long nextTimestamp = Long.parseLong(timestampPrefixedLines[i].split("\\]")[0]);
      assertTrue(currTimestamp <= nextTimestamp);
      currTimestamp = nextTimestamp;
    }
  }

  @Test
  public void testShouldRetrieveBasedOnTimestamp() throws Exception {
    // Run list model repository.
    final int status = runTool(new ListModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(),
            "--timestamp=3..", });
    assertEquals(BaseTool.SUCCESS, status);
    // Only "recommend", "fraudscore", and "incomplete" models should be listed.
    assertEquals(2 * 4 + 3, mToolOutputLines.length);
  }

  @Test
  public void testShouldRetrieveBasedOnLimitedVersions() throws Exception {
    // Run list model repository.
    final int status = runTool(new ListModelRepoTool(),
        new String[] {"--kiji=" + mKiji.getURI().toString(),
            "--artifact=incomplete", "--max-versions=3", "--production-ready", });
    assertEquals(BaseTool.SUCCESS, status);
    // 3 lines for group, artifact, version, and 3 lines for 3 versions of production_ready flag.
    assertEquals(6, mToolOutputLines.length);
  }
}
