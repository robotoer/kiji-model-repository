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
import java.io.PrintWriter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.{ Map => MutableMap }
import scala.io.Source
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.slf4j.LoggerFactory
import com.google.common.io.Files
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.ModelLifeCycle
import org.kiji.modelrepo.ArtifactName
import org.kiji.modelrepo.uploader.MavenArtifactName
import scala.collection.immutable.ListMap
import com.google.common.base.Preconditions

/**
 * Performs the actual deployment/undeployment of model lifecycles by scanning the model
 * repository for changes. The scanner will download any necessary artifacts from the
 * model repository and deploy the necessary files so that the application is available for
 * remote scoring.
 */
class ModelRepoScanner(mKijiModelRepo: KijiModelRepository,
                       mScanIntervalSeconds: Int,
                       mBaseDir: File) extends Runnable {

  val LOG = LoggerFactory.getLogger(classOf[ModelRepoScanner])

  // Setup some constants that will get used when generating the various files to deploy
  // the lifecycle.
  val CONTEXT_PATH = "CONTEXT_PATH"
  val MODEL_NAME = "MODEL_NAME"
  val MODEL_GROUP = "MODEL_GROUP"
  val MODEL_ARTIFACT = "MODEL_ARTIFACT"
  val MODEL_VERSION = "MODEL_VERSION"
  val MODEL_REPO_URI = "MODEL_REPO_URI"

  // Some file constants
  val OVERLAY_FILE = "/org/kiji/scoring/server/instance/overlay.xml"
  val WEB_OVERLAY_FILE = "/org/kiji/scoring/server/instance/web-overlay.xml"
  val JETTY_TEMPLATE_FILE = "/org/kiji/scoring/server/template/template.xml"

  var mIsRunning = false

  /**
   *  Stores a set of deployed lifecycles (identified by group.artifact-version) mapping
   *  to the actual folder that houses this lifecycle.
   */
  private val mArtifactToInstanceDir = MutableMap[ArtifactName, File]()

  /**
   *  Stores a map of model artifact locations to their corresponding template name so that
   *  instances can be properly mapped when created against a war file that has already
   *  been previously deployed. The key is the location string from the location column
   *  in the model repo and the value is the fully qualified name of the lifecycle
   *  (group.artifact-version) to which this location is mapped to.
   */
  private val mDeployedWarFiles = MutableMap[String, String]()

  initializeState

  /**
   * Initializes the state of the internal maps from disk but going through the
   * templates and webapps folders to populate different maps. Also will clean up files/folders
   * that are not valid.
   *
   * This is called upon construction of the ModelRepoScanner and can't be called when the scanner
   * is running.
   */
  private def initializeState() {

    if(mIsRunning) {
      throw new IllegalStateException("Can not initialize state while scanner is running.")
    }

    // This will load up the in memory data structures for the scoring server
    // based on information from disk. First go through the webapps and any webapp that doesn't
    // have a corresponding location file, delete.

    // Any undeploys will happen in the checkForUpdates call at the end.
    val templatesDir = getTemplatesFolder
    val webappsDir = getWebappsFolder
    val instancesDir = getInstancesFolder

    // Validate webapps
    val (validWarFiles, invalidWarFiles) = webappsDir.listFiles().partition(f => {
      val locationFile = new File(webappsDir, f.getName() + ".loc")
      locationFile.exists() && f.getName().endsWith(".war") && f.isFile()
    })

    invalidWarFiles.foreach(f => {
      delete(f)
    })

    // Validate the templates to make sure that they are pointing to a valid
    // war file.
    val (validTemplates, invalidTemplates) = templatesDir.listFiles()
      .partition(templateDir => {
        val (templateName, warBaseName) = parseDirectoryName(templateDir.getName())
        // For a template to be valid, it must have a name and warBaseName AND the
        // warBaseName.war must exist AND warBaseName.war.loc must also exist
        val warFile = new File(webappsDir, warBaseName.getOrElse("") + ".war")
        val locFile = new File(webappsDir, warBaseName.getOrElse("") + ".war.loc")

        !warBaseName.isEmpty && warFile.exists() && locFile.exists() && templateDir.isDirectory()
      })

    invalidTemplates.foreach(template => {
      LOG.info("Deleting invalid template directory " + template)
      delete(template)
    })

    validTemplates.foreach(template => {
      val (templateName, warBaseName) = parseDirectoryName(template.getName())
      val locFile = new File(webappsDir, warBaseName.get + ".war.loc")
      val location = getLocationInformation(locFile)
      mDeployedWarFiles.put(location, templateName)
    })

    // Loop through the instances and add them to the map.
    instancesDir.listFiles().foreach(instance => {
      //templateName=artifactFullyQualifiedName
      val (templateName, artifactName) = parseDirectoryName(instance.getName())

      // This is an inefficient lookup on validTemplates but it's a one time thing on
      // startup of the server scanner.
      if (!instance.isDirectory() || artifactName.isEmpty
        || !validTemplates.contains(templateName)) {
        LOG.info("Deleting invalid instance " + instance.getPath())
        delete(instance)
      } else {
        try {
          val parsedArtifact = new ArtifactName(artifactName.get)
          mArtifactToInstanceDir.put(parsedArtifact, instance)
        } catch {
          case ex => delete(instance)
        }
      }
    })

    checkForUpdates

    mIsRunning = true
  }

  /**
   * Deletes a given file depending on whether it's an actual file or a directory.
   * @param file is the file/folder to delete.
   */
  private def delete(file: File) {
    if (file.isDirectory()) {
      FileUtils.deleteDirectory(file)
    } else {
      file.delete()
    }
  }

  /**
   * Parses a Jetty template/instance directory using the "=" as the delimiter. Returns
   * the two parts that comprise the directory.
   *
   * @param inputDirectory is the directory name to parse.
   * @return a 2-tuple containing the two strings on either side of the "=" operator. If there
   *     "=" in the string, then the second part will be None.
   */
  private def parseDirectoryName(inputDirectory: String): (String, Option[String]) = {
    val parts = inputDirectory.split("=")
    if (parts.length == 1) {
      (parts(0), None)
    } else {
      (parts(0), Some(parts(1)))
    }
  }

  /**
   * Turns off the internal run flag to safely stop the scanning of the model repository
   * table.
   */
  def shutdown() {
    mIsRunning = false
  }

  def run() {
    while (mIsRunning) {
      LOG.debug("Scanning model repository for changes...")
      checkForUpdates
      Thread.sleep(mScanIntervalSeconds * 1000)
    }
  }

  /**
   * Checks the model repository table for updates and deploys/undeploys lifecycles
   * as necessary.
   *
   * @return a two-tuple of the number of lifecycles deployed and undeployed
   */
  def checkForUpdates: (Int, Int) = {
    val allEnabledLifecycles = getAllEnabledLifecycles.asScala.
      foldLeft(ListMap[ArtifactName, ModelLifeCycle]())((currentMap, lifecycle) => {
        currentMap + (lifecycle.getArtifactName() -> lifecycle)
      })

    // Split the lifecycle map into those that are already deployed and those that
    // should be undeployed (based on whether or not the currently enabled lifecycles
    // contain the deployed lifecycle.
    val (toKeep, toUndeploy) =
      mArtifactToInstanceDir.partition(kv => allEnabledLifecycles.contains(kv._1))

    // For each lifecycle to undeploy, remove it.
    toUndeploy.map(kv => {
      val (artifact, location) = kv

      LOG.info("Undeploying lifecycle " + artifact.getFullyQualifiedName() +
        " location = " + location)
      FileUtils.deleteDirectory(location)
    })

    // Now find the set of lifecycles to add by diffing the current with the already
    // deployed and add those.
    val toDeploy = allEnabledLifecycles.keySet.diff(toKeep.keySet)
    toDeploy.map(artifact => {
      val lifecycle = allEnabledLifecycles(artifact)
      LOG.info("Deploying artifact " + artifact.getFullyQualifiedName())
      deployArtifact(lifecycle)
    })

    (toDeploy.size, toUndeploy.size)
  }

  /**
   * Deploys the specified model artifact by either creating a new Jetty instance or by
   * downloading the artifact and setting up a new template/instance in Jetty.
   *
   * @param artifact is the specified ModelLifeCycle to deploy.
   */
  private def deployArtifact(lifecycle: ModelLifeCycle) = {

    val mavenArtifact = new MavenArtifactName(lifecycle.getArtifactName())
    val artifact = lifecycle.getArtifactName()

    // Deploying requires a few things.
    // If we have deployed this artifact's war file before:
    val fullyQualifiedName = artifact.getFullyQualifiedName()

    val contextPath = (mavenArtifact.getGroupName() + "/" +
      mavenArtifact.getArtifactName()).replace('.', '/') + "/" + artifact.getVersion().toString()
    // Populate a map of the various placeholder values to substitute in files
    val templateParamValues = Map[String, String](
      MODEL_ARTIFACT -> mavenArtifact.getArtifactName(),
      MODEL_GROUP -> mavenArtifact.getGroupName(),
      MODEL_NAME -> artifact.getFullyQualifiedName(),
      MODEL_VERSION -> artifact.getVersion().toString(),
      MODEL_REPO_URI -> mKijiModelRepo.getURI().toString(),
      CONTEXT_PATH -> contextPath)

    if (mDeployedWarFiles.contains(lifecycle.getLocation())) {
      // 1) Create a new instance and done.
      createNewInstance(lifecycle, mDeployedWarFiles.get(lifecycle.getLocation()).get,
        templateParamValues)
    } else {
      // If not:
      // 1) Download the artifact to a temporary location
      val artifactFile = File.createTempFile("artifact", "war")
      // The artifact downloaded should reflect the name of the file that was uploaded
      val finalArtifactName = String.format("%s.%s", mavenArtifact.getGroupName(),
        FilenameUtils.getName(lifecycle.getLocation()))

      lifecycle.downloadArtifact(artifactFile)

      // 2) Create a new Jetty template to map to the war file
      // Template is (fullyQualifiedName=warFileBase)
      val templateDirName = String.format("%s=%s", fullyQualifiedName,
        FilenameUtils.getBaseName(finalArtifactName));

      val tempTemplateDir = new File(Files.createTempDir(), "WEB-INF")
      tempTemplateDir.mkdirs()

      translateFile(JETTY_TEMPLATE_FILE, new File(tempTemplateDir, "template.xml"),
        Map[String, String]())

      // Move the temporarily downloaded artifact to its final location.
      artifactFile.renameTo(new File(getWebappsFolder, finalArtifactName))

      // As part of the state necessary to reconstruct the scoring server on cold start, write
      // out the location of the lifecycle (which is used to determine if a war file needs
      // to actually be deployed when a lifecycle is deployed) to a text file.
      writeLocationInformation(finalArtifactName, lifecycle)

      val moveResult = tempTemplateDir.getParentFile().
        renameTo(new File(getTemplatesFolder, templateDirName))

      // 3) Create a new instance.
      createNewInstance(lifecycle, fullyQualifiedName, templateParamValues)

      mDeployedWarFiles.put(lifecycle.getLocation(), fullyQualifiedName)
    }
  }

  /**
   * Writes out the relative URI of the lifecycle (in the model repository) to a known
   * text file in the webapps folder. This is used later on server reboot to make sure that
   * currently enabled and previously deployed lifecycles are represented in the internal
   * server state. The file name will be <artifactFileName>.loc.
   *
   * @param artifactFileName is the name of the artifact file name that is being locally deployed.
   * @param lifecycle is the model lifecycle object that is associated with the artifact.
   */
  private def writeLocationInformation(artifactFileName: String, lifecycle: ModelLifeCycle) = {
    val locationFile = artifactFileName + ".loc"
    val locationWriter = new PrintWriter(new File(getWebappsFolder, locationFile))
    locationWriter.println(lifecycle.getLocation())
    locationWriter.close()
  }

  /**
   * Returns the location information from the specified file. The input file is the name of the
   * location file that was written using the writeLocationInformation method.
   *
   * @param locationFile is the name of the location file containing the lifecycle location
   *                     information.
   * @return the location information in the file.
   */
  private def getLocationInformation(locationFile: File): String = {
    val inputSource = Source.fromFile(locationFile)
    val location = inputSource.mkString.trim()
    inputSource.close
    location
  }

  /**
   * Creates a new Jetty overlay instance.
   *
   * @param artifact is the ModelLifeCycle to deploy.
   * @param templateName is the name of the template to which this instance belongs.
   * @param bookmarkParams contains a map of parameters and values used when configuring
   *        the WEB-INF specific files. An example of a parameter includes the context name
   *        used when addressing this lifecycle via HTTP which is dynamically populated based
   *        on the fully qualified name of the ModelLifeCycle.
   */
  private def createNewInstance(lifecycle: ModelLifeCycle,
                                templateName: String,
                                bookmarkParams: Map[String, String]) = {
    // This will create a new instance by leveraging the template files on the classpath
    // and create the right directory. Maybe first create the directory in a temp location and
    // move to the right place.
    val artifact = lifecycle.getArtifactName()

    val tempInstanceDir = new File(Files.createTempDir(), "WEB-INF")
    tempInstanceDir.mkdir()

    // templateName=artifactFullyQualifiedName
    val instanceDirName = String.format("%s=%s", templateName,
      artifact.getFullyQualifiedName())

    translateFile(OVERLAY_FILE, new File(tempInstanceDir, "overlay.xml"), bookmarkParams)
    translateFile(WEB_OVERLAY_FILE, new File(tempInstanceDir, "web-overlay.xml"), bookmarkParams)

    val finalInstanceDir = new File(getInstancesFolder, instanceDirName)

    tempInstanceDir.getParentFile().renameTo(finalInstanceDir)

    mArtifactToInstanceDir.put(artifact, finalInstanceDir)
    FileUtils.deleteDirectory(tempInstanceDir)
  }

  /**
   * Given a "template" WEB-INF .xml file on the classpath, this will produce a translated
   * version of the file replacing any "bookmark" values (i.e. "%PARAM%") with their actual values
   * which are dynamically generated based on the artifact being deployed.
   *
   * @param filePath is the path to the xml file on the classpath (bundled with this scoring server)
   * @param targetFile is the path where the file is going to be written.
   * @param bookmarkParams contains a map of parameters and values used when configuring
   *        the WEB-INF specific files. An example of a parameter includes the context name
   *        used when addressing this lifecycle via HTTP which is dynamically populated based
   *        on the fully qualified name of the ModelLifeCycle.
   */
  private def translateFile(filePath: String,
                            targetFile: File,
                            bookmarkParams: Map[String, String]) {
    val fileStream = Source.fromInputStream(getClass.getResourceAsStream(filePath))
    val fileWriter = new PrintWriter(targetFile)

    fileStream.getLines.foreach(line => {
      val newLine =
        if (line.matches(".*?%[A-Z_]+%.*?")) {
          bookmarkParams.foldLeft(line) { (result, currentKV) =>
            val (key, value) = currentKV
            result.replace("%" + key + "%", value)
          }
        } else {
          line
        }
      fileWriter.println(newLine)
    })

    fileWriter.close()
  }

  /**
   * Returns all the currently enabled lifecycles from the model repository.
   * @return all the currently enabled lifecycles from the model repository.
   */
  private def getAllEnabledLifecycles = {
    mKijiModelRepo.getModelLifeCycles(null, 1, true)
  }

  /**
   * Returns the webapps folder relative to the base directory of the server.
   *
   * @return the webapps folder relative to the base directory of the server.
   */
  def getWebappsFolder: File = {
    val webappsFolder = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.WEBAPPS)
    new File(mBaseDir, webappsFolder.getPath())
  }

  /**
   * Returns the instances folder relative to the base directory of the server.
   *
   * @return the instances folder relative to the base directory of the server.
   */
  def getInstancesFolder: File = {
    val instancesFolder = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.INSTANCES)
    new File(mBaseDir, instancesFolder.getPath())
  }

  /**
   * Returns the templates folder relative to the base directory of the server.
   *
   * @return the templates folder relative to the base directory of the server.
   */
  def getTemplatesFolder: File = {
    val templatesFolder = new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.TEMPLATES)
    new File(mBaseDir, templatesFolder.getPath())
  }
}
