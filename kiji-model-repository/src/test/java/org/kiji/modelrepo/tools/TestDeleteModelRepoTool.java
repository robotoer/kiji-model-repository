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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.junit.Test;

import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.schema.Kiji;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolTest;

public class TestDeleteModelRepoTool extends KijiToolTest {

  @Test
  public void testShouldInstallThenDeleteModelRepo() throws Exception {
    final Kiji localKiji = getKiji();
    final URI baseRepoUrl = new URI("http://someHost:1234/releases");
    KijiModelRepository.install(localKiji, baseRepoUrl);
    final String kijiArg = String.format("--kiji=%s", localKiji.getURI().toString());
    final int returnCode = runTool(new DeleteModelRepoTool(), kijiArg);
    assertFalse(localKiji.getTableNames()
        .contains(KijiModelRepository.MODEL_REPO_TABLE_NAME));
    assertEquals(BaseTool.SUCCESS, returnCode);
  }

  @Test
  public void testDeleteModelWhereNoneExists() throws Exception {
    final Kiji localKiji = getKiji();
    assertFalse(localKiji.getTableNames()
        .contains(KijiModelRepository.MODEL_REPO_TABLE_NAME));
    try {
      final String kijiArg = String.format("--kiji=%s", localKiji.getURI().toString());
      runTool(new DeleteModelRepoTool(), kijiArg);
      fail("Deleting model repo succeeded when it should have failed.");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("Model repository which is to be deleted "));
    }
    assertFalse(localKiji.getTableNames()
        .contains(KijiModelRepository.MODEL_REPO_TABLE_NAME));
  }

  @Test
  public void testDeleteInvalidModelRepoTable() throws Exception {
    final Kiji localKiji = getKiji();
    final TableLayoutDesc fakeModelRepoTable = KijiTableLayouts
        .getLayout(KijiTableLayouts.SIMPLE_UNHASHED);
    fakeModelRepoTable.setName(KijiModelRepository.MODEL_REPO_TABLE_NAME);
    localKiji.createTable(fakeModelRepoTable);
    assertTrue(localKiji.getTableNames()
        .contains(KijiModelRepository.MODEL_REPO_TABLE_NAME));
    try {
      final String kijiArg = String.format("--kiji=%s", localKiji.getURI().toString());
      runTool(new DeleteModelRepoTool(), kijiArg);
      fail("Deleting model repo succeeded when it should have failed.");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("Expected model repository table is not a valid"));
    }
    assertTrue(localKiji.getTableNames()
        .contains(KijiModelRepository.MODEL_REPO_TABLE_NAME));
  }
}
