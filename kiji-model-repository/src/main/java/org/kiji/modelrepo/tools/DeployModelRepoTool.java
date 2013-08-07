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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;

import org.kiji.common.flags.Flag;
import org.kiji.express.avro.AvroModelDefinition;
import org.kiji.express.avro.AvroModelEnvironment;
import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.ProtocolVersion;

/**
 * The deploy tool uploads the model lifecycle and coordinates to the model repository.
 */
public class DeployModelRepoTool extends BaseTool implements KijiModelRepoTool {
  /** Maven artifact name: groupId.artifactId. */
  private String mMavenArtifact = null;

  @Flag(name = "kiji", usage = "Kiji instance housing the model repository.")
  private String mInstanceURIFlag = KConstants.DEFAULT_INSTANCE_URI;

  private KijiURI mInstanceURI = null;

  @Flag(name="definition", usage="Path to model definition.")
  private String mDefinitionFlag = null;

  @Flag(name="environment", usage="Path to model environment.")
  private String mEnvironmentFlag = null;

  @Flag(name="version", usage="Model lifecycle version.")
  private String mVersionFlag = null;

  @Flag(name="production_ready", usage="Is the model lifecycle production ready.")
  private boolean mProductionReady = false;

  @Flag(name="message", usage="Update message for this deployment.")
  private String mMessage = null;

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return MODEL_REPO_TOOL_CATEGORY;
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Upload a new version of a lifecycle and/or code artifact.";
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return MODEL_REPO_TOOL_BASE + getModelRepoToolName();
  }

  /** {@inheritDoc} */
  @Override
  public String getModelRepoToolName() {
    return "deploy";
  }

  protected void validateFlags() throws Exception {
    Preconditions.checkNotNull(mDefinitionFlag,
        "Specify a model definition with --definition=modeldefinition.json ");
    Preconditions.checkNotNull(mEnvironmentFlag,
        "Specify a model environment with --environment=modelenv.json");
    Preconditions.checkNotNull(mMessage,
        "Specify an update message for this deployment with --message=updatemessage");

    mInstanceURI = KijiURI.newBuilder(mInstanceURIFlag).build();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> arg0) throws Exception {
    // Validate that there's a maven artifact specified.
    Preconditions.checkArgument(arg0.size() == 1, "Specify the name of exactly one artifact: Found: {}", arg0.size());
    mMavenArtifact = arg0.get(0);

    Kiji kiji = Kiji.Factory.open(mInstanceURI);
    KijiModelRepository kijiModelRepository = KijiModelRepository.open(kiji);

    // TODO: Determine if this is the right version to put in.
    // Maven uses x.y.z-qualifier, whereas ProtocolVersion doesn't support qualifiers.
    ProtocolVersion protocolVersion = ProtocolVersion.parse(mVersionFlag);

    AvroModelDefinition avroModelDefinition = readAvroModelDefinition(mDefinitionFlag);
    AvroModelEnvironment avroModelEnvironment = readAvroModelEnvironment(mEnvironmentFlag);

    kijiModelRepository.deployModelLifecycle(mMavenArtifact,
        protocolVersion,
        avroModelDefinition,
        avroModelEnvironment,
        mProductionReady,
        mMessage);
    return SUCCESS;
  }

  /**
   * Reads an AvroModelDefinition from the specified file.
   *
   * @param filename containing the AvroModelDefinition.
   * @return AvroModelDefinition parsed from the file.
   */
  private static AvroModelDefinition readAvroModelDefinition(String filename) throws IOException {
    String json = readJSONFromFile(filename);
    return (AvroModelDefinition) FromJson.fromJsonString(json, AvroModelDefinition.SCHEMA$);
  }

  /**
   * Reads an AvroModelEnvironment from the specified file.
   *
   * @param filename containing the AvroModelEnvironment.
   * @return AvroModelEnvironment parsed from the file.
   */
  private static AvroModelEnvironment readAvroModelEnvironment(String filename) throws IOException {
    String json = readJSONFromFile(filename);
    return (AvroModelEnvironment) FromJson.fromJsonString(json, AvroModelEnvironment.SCHEMA$);
  }

  /**
   * Reads JSON from a file as a single string.  Doesn't actually have to be a proper JSON, but
   * this is a helper method for the other classes.
   *
   * @param filename to read from
   * @return String containing the JSON contents of the file.
   */
  private static String readJSONFromFile(String filename) throws IOException {
    FileInputStream fis = new FileInputStream(filename);
    return IOUtils.toString(fis);
  }


}
