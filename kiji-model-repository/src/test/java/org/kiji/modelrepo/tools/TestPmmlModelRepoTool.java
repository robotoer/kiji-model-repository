/**
 * (c) Copyright 2014 WibiData, Inc.
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

import org.junit.Test;

import org.kiji.schema.tools.KijiToolTest;

public class TestPmmlModelRepoTool extends KijiToolTest {
  @Test
  public void testGenerateModelContainer() {
    // Write test PMML file to a temporary location.
    
    // Call the tool.
    // Validate the resulting model container.
  }

  @Test
  public void testValidateFlagExistence() {
    // Test each required flag as missing (should error).
    // Test each optional flag as missing (should not error).
  }

  @Test
  public void testValidateFileExistence() {
    // Test missing pmml file.
    // Test invalid pmml file (is a directory).

    // Test model container already exists.
  }

  @Test
  public void testValidateTableExistence() {
    // Test missing table.
    // Test missing predictor column.
    // Test missing result column.
  }
}
