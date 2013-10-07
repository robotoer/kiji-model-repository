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

import java.io.File

import org.eclipse.jetty.deploy.DeploymentManager
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.eclipse.jetty.server.AbstractConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerCollection

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

case class ServerConfiguration(
  port: Int,
  repo_uri: String,
  repo_scan_interval: Int,
  num_acceptors: Int)

/**
 * Scoring Server class. Provides a few wrappers around the Jetty server underneath.
 *
 * @param mBaseDir is the base directory in which the expected models and conf directories are
 *     expected to exist.
 * @param mServerConfig is the scoring server configuration containing information such as the Kiji
 *     URI.
 */
class ScoringServer(mBaseDir: File, mServerConfig: ServerConfiguration) {

  val kijiURI = KijiURI.newBuilder(mServerConfig.repo_uri).build()
  val kiji = Kiji.Factory.open(kijiURI)
  val kijiModelRepo = KijiModelRepository.open(kiji)

  // Start the model lifecycle scanner thread that will scan the model repository
  // for changes.
  val lifeCycleScanner = new ModelRepoScanner(
    kijiModelRepo,
    mServerConfig.repo_scan_interval,
    mBaseDir)
  val lifeCycleScannerThread = new Thread(lifeCycleScanner)
  lifeCycleScannerThread.start()

  val server = new Server(mServerConfig.port)

  // Increase the number of acceptor threads.
  val connector = server.getConnectors()(0).asInstanceOf[AbstractConnector]
  connector.setAcceptors(mServerConfig.num_acceptors)

  val handlers = new HandlerCollection()

  val contextHandler = new ContextHandlerCollection()
  val deploymentManager = new DeploymentManager()
  val overlayedProvider = new OverlayedAppProvider

  overlayedProvider.setScanDir(new File(mBaseDir, ScoringServer.MODELS_FOLDER))
  // For now scan this directory once per second.
  overlayedProvider.setScanInterval(1)

  deploymentManager.setContexts(contextHandler)
  deploymentManager.addAppProvider(overlayedProvider)

  handlers.addHandler(contextHandler)
  handlers.addHandler(new DefaultHandler())

  server.setHandler(handlers);
  server.addBean(deploymentManager)

  def start() {
    server.start()
  }

  def stop() {
    server.stop()
  }

  def releaseResources() {
    lifeCycleScanner.shutdown
    kijiModelRepo.close()
  }
}

/**
 * Main entry point for the scoring server. This pulls in and combines various Jetty components
 * to boot a new web server listening for scoring requests.
 */
object ScoringServer {

  val CONF_FILE = "configuration.json"
  val MODELS_FOLDER = "models"
  val LOGS_FOLDER = "logs"
  val CONF_FOLDER = "conf"

  def main(args: Array[String]): Unit = {

    // Check that we started in the right location else bomb out
    if (!startedInProperLocation) {
      System.err.println("Server not started in proper location. Exiting...")
      return
    }

    val scoringServer = ScoringServer(null)

    // Gracefully shutdown the deployment thread to let it finish anything that it may be doing
    sys.ShutdownHookThread {
      scoringServer.releaseResources
    }

    scoringServer.start()
    scoringServer.server.join();
  }

  /**
   * Constructs a Jetty Server instance configured using the conf/configuration.json.
   * @param baseDir
   * @return a constructed Jetty server.
   */
  def apply(baseDir: File): ScoringServer = {
    val confFile = new File(baseDir, String.format("%s/%s", CONF_FOLDER, CONF_FILE))
    val config = getConfig(confFile)

    new ScoringServer(baseDir, config)
  }

  /**
   * Checks that the server is started in the right location by ensuring the presence of a few key
   * directories under the conf, models and logs folder.
   *
   * @return whether or not the key set of folders exist or not.
   */
  def startedInProperLocation: Boolean = {
    val filesToCheck = Array(CONF_FOLDER + "/" + CONF_FILE, MODELS_FOLDER + "/webapps",
      MODELS_FOLDER + "/instances", MODELS_FOLDER + "/templates", LOGS_FOLDER)

    for (file <- filesToCheck) {
      val fileObj = new File(file.toString())
      if (!fileObj.exists()) {
        System.err.println("Error: " + file + " does not exist!")
        return false
      }
    }
    return true
  }

  /**
   * Returns the ServerConfiguration object constructed from conf/configuration.json.
   *
   * @param confFile is the location of the configuration used to configure the server.
   * @return the ServerConfiguration object constructed from conf/configuration.json.
   */
  def getConfig(confFile: File): ServerConfiguration = {
    val configMapper = new ObjectMapper
    configMapper.registerModule(DefaultScalaModule)
    configMapper.readValue(confFile, classOf[ServerConfiguration])
  }
}
