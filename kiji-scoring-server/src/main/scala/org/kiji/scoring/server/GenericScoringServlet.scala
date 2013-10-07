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

package org.kiji.scoring.server

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.util.HashMap

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Preconditions

import org.apache.hadoop.conf.Configuration

import org.kiji.express.avro.AvroInputSpec
import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.framework.ScoreProducer
import org.kiji.mapreduce.kvstore.KeyValueStore
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigValidator
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.modelrepo.ArtifactName
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiURI
import org.kiji.schema.tools.ToolUtils
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson
import org.kiji.web.KijiWebContext

/**
 * Servlet implementation that executes the scoring phase of a model lifecycle deployed
 * from the model repository. The environment and definition configuration is used to construct
 * the appropriate phase implementation objects and the producer wrapping this performs the
 * actual execution.
 */
class GenericScoringServlet extends HttpServlet {

  var mInputTable: String = null
  var mInputKiji: Kiji = null
  var mProducerContext: KijiWebContext = null
  var mScoreProducer: KijiProducer = null

  val MODEL_REPO_URI = "model-repo-uri"
  val MODEL_GROUP = "model-group"
  val MODEL_ARTIFACT = "model-artifact"
  val MODEL_VERSION = "model-version"

  override def init() {
    val uri = getServletConfig().getInitParameter(MODEL_REPO_URI)
    val modelRepoURI = KijiURI.newBuilder(uri.toString()).build()
    val kiji = Kiji.Factory.open(modelRepoURI)

    val modelName = getServletConfig().getInitParameter(MODEL_GROUP)
    val modelArtifact = getServletConfig().getInitParameter(MODEL_ARTIFACT)
    val modelVersion = ProtocolVersion.parse(getServletConfig().getInitParameter(MODEL_VERSION))

    try {
      // Fetch the model def/env from the repo
      val modelRepo = KijiModelRepository.open(kiji)

      val lifeCycleRow = modelRepo.getModelLifeCycle(
        new ArtifactName(String.format("%s.%s", modelName, modelArtifact), modelVersion))

      val definitionAvro: AvroModelDefinition = lifeCycleRow.getDefinition()
      val environmentAvro: AvroModelEnvironment = lifeCycleRow.getEnvironment()

      // TODO: Is is alright that I am getting the input spec directly through the Avro
      // record as opposed to through the KijiExpress wrapper classes?
      val inputConfig: AvroInputSpec = environmentAvro.getScoreEnvironment()
        .getInputSpec()
        .asInstanceOf[AvroInputSpec]

      val uri = inputConfig.getKijiSpecification().getTableUri()

      val inputKijiURI = KijiURI.newBuilder(uri).build()
      mInputKiji = Kiji.Factory.open(inputKijiURI)
      mInputTable = inputKijiURI.getTable()

      val defAvro = ToJson.toJsonString(definitionAvro)
      val envAvro = ToJson.toJsonString(environmentAvro)

      val modelEnvironment = ModelEnvironment.fromJson(envAvro)
      val modelDefinition = ModelDefinition.fromJson(defAvro)

      modelRepo.close()

      mScoreProducer = new ScoreProducer()
      val conf = new Configuration()

      conf.set(ScoreProducer.modelDefinitionConfKey, modelDefinition.toJson)
      conf.set(ScoreProducer.modelEnvironmentConfKey, modelEnvironment.toJson)
      mScoreProducer.setConf(conf)

      val boundKVStores = new HashMap[String, KeyValueStore[_, _]]
      KeyValueStoreConfigValidator.get().bindAndValidateRequiredStores(
        mScoreProducer.getRequiredStores(), boundKVStores)
      mProducerContext = new KijiWebContext(boundKVStores,
          new KijiColumnName(mScoreProducer.getOutputColumn))
      mScoreProducer.setup(mProducerContext)

    } finally {
      kiji.release()
    }
  }

  override def destroy() {
    mInputKiji.release()
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {

    // Fetch the entity_id parameter from the URL. Fail if not specified.
    val eid = req.getParameter("eid")
    Preconditions.checkNotNull(eid, "Entity ID required!", "")

    // Open the writer to send the data back.
    val jsonOutputWriter = new BufferedWriter(new OutputStreamWriter(resp.getOutputStream()))
    try {
      val table = mInputKiji.openTable(mInputTable)
      val entityId = ToolUtils.createEntityIdFromUserInputs(eid, table.getLayout());

      // Fetch the row given the data request specified in the environment
      val dataReq = mScoreProducer.getDataRequest()
      val reader = table.openTableReader()
      val rowData = reader.get(entityId, dataReq)
      // TODO: How to know if rowdata is empty to avoid calling score?
      reader.close()

      mScoreProducer.produce(rowData, mProducerContext)

      val mapper = new ObjectMapper()
      jsonOutputWriter.write(mapper.valueToTree(mProducerContext.getWrittenCell()).toString())
      table.release()
    } finally {
      jsonOutputWriter.close()
    }
  }
}
