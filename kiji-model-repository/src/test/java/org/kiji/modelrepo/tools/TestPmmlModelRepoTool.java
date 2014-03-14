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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import org.kiji.modelrepo.avro.KijiModelContainer;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolTest;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.Resources;
import org.kiji.scoring.lib.JpmmlScoreFunction;

public class TestPmmlModelRepoTool extends KijiToolTest {
  @Test
  public void testGenerateModelContainer() throws Exception {
    // Write test PMML file to a temporary location.
    final File pmmlModelFile = File.createTempFile(
        TestPmmlModelRepoTool.class.getName() + "-simple-linear-regression",
        ".xml"
    );
    {
      final String pmmlModelString =
          IOUtils.toString(Resources.openSystemResource("simple-linear-regression.xml"));
      pmmlModelFile.deleteOnExit();
      final PrintWriter pmmlPrintWriter = new PrintWriter(pmmlModelFile);
      try {
        pmmlPrintWriter.println(pmmlModelString);
      } finally {
        pmmlPrintWriter.flush();
        pmmlPrintWriter.close();
      }
    }

    // Create a temporary output location.
    final File modelContainer = Files.createTempDir();

    // Setup parameters for score function.
    final String kijiUriString = "kiji://.env/default/test";
    final String modelFile = "file://" + pmmlModelFile.getAbsolutePath();
    final String modelName = "linearreg";
    final String modelVersion = "0.1.0";
    final KijiColumnName predictorColumn = new KijiColumnName("model", "predictor");
    final KijiColumnName resultColumn = new KijiColumnName("model", "result");
    final String resultRecordName = "SimpleLinearRegressionPredicted";
    final String modelContainerPath =
        String.format("%s/linearreg.json", modelContainer.getAbsolutePath());

    // Call the tool.
    final int status = runTool(new PmmlModelRepoTool(),
        String.format("--table=%s", kijiUriString),
        String.format("--model-file=%s", modelFile),
        String.format("--model-name=%s", modelName),
        String.format("--model-version=%s", modelVersion),
        String.format("--predictor-column=%s", predictorColumn.getName()),
        String.format("--result-column=%s", resultColumn.getName()),
        String.format("--result-record-name=%s", resultRecordName),
        String.format("--model-container=%s", modelContainerPath)
    );
    assertEquals(BaseTool.SUCCESS, status);

    // Validate the resulting model container.
    final File modelContainerFile = new File(modelContainerPath);
    assertTrue(
        "Output model container file didn't get created!",
        modelContainerFile.exists() && modelContainerFile.isFile()
    );

    // Load the resulting model container.
    final KijiModelContainer container = (KijiModelContainer) FromJson.fromJsonString(
        FileUtils.readFileToString(modelContainerFile),
        KijiModelContainer.getClassSchema()
    );

    // Check all of the key/value pairs.
    final Map<String, String> modelParameters = container.getParameters();
    modelParameters.containsKey(JpmmlScoreFunction.MODEL_FILE_PARAMETER);
    modelParameters.containsKey(JpmmlScoreFunction.MODEL_NAME_PARAMETER);
    modelParameters.containsKey(JpmmlScoreFunction.PREDICTOR_COLUMN_PARAMETER);
    modelParameters.containsKey(JpmmlScoreFunction.RESULT_RECORD_PARAMETER);

    assertEquals(
        modelFile,
        modelParameters.get(JpmmlScoreFunction.MODEL_FILE_PARAMETER)
    );
    assertEquals(
        modelName,
        modelParameters.get(JpmmlScoreFunction.MODEL_NAME_PARAMETER)
    );
    assertEquals(
        predictorColumn.getName(),
        modelParameters.get(JpmmlScoreFunction.PREDICTOR_COLUMN_PARAMETER)
    );
    assertEquals(
        resultRecordName,
        modelParameters.get(JpmmlScoreFunction.RESULT_RECORD_PARAMETER)
    );
    assertEquals(
        resultColumn.getName(),
        container.getColumnName()
    );
    assertEquals(
        modelName,
        container.getModelName()
    );
    assertEquals(
        modelVersion,
        container.getModelVersion()
    );
    assertEquals(
        JpmmlScoreFunction.class.getName(),
        container.getScoreFunctionClass()
    );
    assertEquals(
        kijiUriString,
        container.getTableUri()
    );
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
