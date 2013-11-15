Kiji Model Repository ${project.version}
===========================
The Kiji Model Repository allows developers to store and access machine learning models. Using the Kiji Model Repository, data scientists, engineers, analysts can collaborate and publish their artifacts to the model repository for later use by the real-time scoring engine.

Development Warning
-------------------

This project is still under heavy development and has not yet had a formal release.
The APIs and code in this project may change in severely incompatible ways while we
redesign components for their presentation-ready form.

End users are advised to not depend on any functionality in this repository until a
release is performed. See [the Kiji project homepage](http://www.kiji.org) to download
an existing release of KijiSchema, and follow [@kijiproject](http://twitter.com/kijiproject)
for announcements of future releases, including Kiji Model Repository.

Issues are being tracked at [the Kiji JIRA instance](https://jira.kiji.org/browse/SCHEMA).

Installation
--
The Kiji Model Repository relies on the following:

1. A storage layer which can either be backed by a local (or NFS mounted remote) file system or a Maven repository (Nexus/Artifactory).
2. If the storage layer is Maven and any security needs to be configured, ensure that in the .m2/settings.xml file, the following snippet is present:
<pre>
  &lt;servers&gt;
    &lt;server&gt;
      &lt;id&gt;kiji-model-repository&lt;/id&gt;
      &lt;username&gt;REPO_USER&lt;/username&gt;
      &lt;password&gt;REPO_PASS&lt;/password&gt;
    &lt;/server&gt;
  &lt;/servers&gt;
</pre>
When the Kiji Model Repository command line tools perform any uploads to a remote Maven repository, it will do so to assuming a configuration for server id "kiji-model-repository" to allow for the specification of security credentials.
3. [Kiji Modeling](/kijiproject/kiji-modeling) so that the model repository has access to the classes necessary to parse and understand the model definition and model environment files.

Usage
--

1. Ensure that the KIJI_CLASSPATH has both the Kiji Model Repository jars and the Kiji Modeling jars mentioned above. For example:
<pre>
export KIJI_CLASSPATH=$KIJI_CLASSPATH:$KIJI_HOME/modeling/lib/\*:$KIJI_HOME/model-repo/lib/\*
</pre>
2. Executing `kiji model-repo` should show:
<pre>
model-repo init - Creates a new model repository.
model-repo deploy - Upload a new version of a lifecycle and/or code artifact.
model-repo upgrade - Upgrades a new model repository.
model-repo delete - Removes existing model repository.
model-repo check - Checks model repository for consistency.
model-repo list - Lists models in model repository.
model-repo update - Updates the production ready flag and sets message for models in the model
    repository.
</pre>
3. Install a Kiji instance if you don't have one.
<pre>
kiji install
</pre>
4. Create a directory to house your model repository. Initialize the repo by running
<pre>
kiji model-repo init &lt;url to maven repository&gt;
</pre>
If you want to use a local directory for your maven repository, e.g. /tmp/modelrepo, create it and run the init command with a file url:
<pre>
kiji model-repo init file:///tmp/modelrepo
</pre>
This will create a table named model_repo in Kiji. Use `kiji ls kiji://.env/default` to make sure it exists.
5. Visit the directory containing your lifecycle project (see section below) and package your project if necessary. If you are using the `pom.xml` snippet provided below, then you can generate this jar by doing a `mvn clean package`, being sure to substitute the jar name in the example with the one that you generated. Then deploy:
<pre>
kiji model-repo deploy org.kiji.my-model-1.0.0 target/samplelifecycles-1.0.0.jar \
--definition=conf/lifecycle/model_definition.json \
--environment=conf/lifecycle/model_environment.json \
--message="Initial upload"
</pre>
This sample command assumes that your lifecycle project's jar is named `samplelifecycles-1.0.0.jar` and that its definition and environment live in the conf/lifecycle/ directory. You can specify the name and version that you want to give it, in this case `org.kiji.my-model-1.0.0`.
If you have multiple models to import, you can repeat this step, setting the appropriate model name+version, definition and environment.
6. Use `kiji model-repo list` to verify that your model was imported.
7. If you don't already have a table, you can create one using kiji, and then insert some data - see the [Kiji Get Started guide](http://www.kiji.org/getstarted) for more info. This table should correspond to the table mentioned in the model definition and environment. In our example, this is the user table.
8. You're ready to use the scoring server with your model repository - find more at [Kiji Scoring Server](https://github.com/kijiproject/kiji-scoring-server).

Lifecycle Project
-----------------
A very basic lifecycle project includes the following files:

1. A pom file. This is helpful for building the code artifact required for deployment to the model repository.
2. A model definition and environment. Find more information about the [Model Definition](https://github.com/kijiproject/kiji-modeling/blob/master/src/main/scala/org/kiji/modeling/config/ModelDefinition.scala) and [Model Environment](https://github.com/kijiproject/kiji-modeling/blob/master/src/main/scala/org/kiji/modeling/config/ModelEnvironment.scala) in [Kiji Modeling](/kijiproject/kiji-modeling).
3. Model lifecycle phase implementations. In our example, we have only implemented the extract and score phase, and they are in the same class due to the simplicity of the example.

###Example files:

####pom.xml
<pre><code>&lt;project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd"&gt;

  &lt;modelVersion&gt;4.0.0&lt;/modelVersion&gt;

  &lt;groupId&gt;org.kiji.web&lt;/groupId&gt;
  &lt;version&gt;1.0.0&lt;/version&gt;
  &lt;artifactId&gt;samplelifecycles&lt;/artifactId&gt;
  &lt;packaging&gt;jar&lt;/packaging&gt;

  &lt;name&gt;sample lifecycles&lt;/name&gt;
  &lt;description&gt;Bunches of sample lifecycles&lt;/description&gt;

  &lt;dependencies&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;org.kiji.modeling&lt;/groupId&gt;
      &lt;artifactId&gt;kiji-modeling&lt;/artifactId&gt;
      &lt;version&gt;0.3.0&lt;/version&gt;
      &lt;scope&gt;provided&lt;/scope&gt;
    &lt;/dependency&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;org.kiji.express&lt;/groupId&gt;
      &lt;artifactId&gt;kiji-express&lt;/artifactId&gt;
      &lt;version&gt;0.13.0&lt;/version&gt;
      &lt;scope&gt;provided&lt;/scope&gt;
    &lt;/dependency&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;org.kiji.mapreduce&lt;/groupId&gt;
      &lt;artifactId&gt;kiji-mapreduce&lt;/artifactId&gt;
      &lt;version&gt;1.2.1&lt;/version&gt;
      &lt;scope&gt;provided&lt;/scope&gt;
    &lt;/dependency&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;org.kiji.schema&lt;/groupId&gt;
      &lt;artifactId&gt;kiji-schema&lt;/artifactId&gt;
      &lt;version&gt;1.3.2&lt;/version&gt;
      &lt;scope&gt;provided&lt;/scope&gt;
    &lt;/dependency&gt;
  &lt;/dependencies&gt;

  &lt;repositories&gt;
    &lt;repository&gt;
      &lt;id&gt;kiji-repos&lt;/id&gt;
      &lt;name&gt;kiji-repos&lt;/name&gt;
      &lt;url&gt;https://repo.wibidata.com/artifactory/kiji&lt;/url&gt;
    &lt;/repository&gt;
    &lt;repository&gt;
      &lt;id&gt;kiji-nightly&lt;/id&gt;
      &lt;name&gt;kiji-nightly&lt;/name&gt;
      &lt;url&gt;https://repo.wibidata.com/artifactory/kiji-nightly&lt;/url&gt;
    &lt;/repository&gt;
  &lt;/repositories&gt;
&lt;/project&gt;
</code></pre>

####model_definition.json
<pre><code>{
  "name": "name",
  "version": "1.0.0",
  "scorer_phase": {
    "org.kiji.modeling.avro.AvroPhaseDefinition": {
      "extractor_class":"org.kiji.lifecycle.DummyExtractorScorer",
      "phase_class":"org.kiji.lifecycle.DummyExtractorScorer"
    }
  },
  "protocol_version": "model_definition-0.4.0"
}
</code></pre>

####model_environment.json
<pre><code>{
 "protocol_version":"model_environment-0.4.0",
 "name":"myRunProfile",
 "version":"1.0.0",
 "score_environment":{
   "org.kiji.modeling.avro.AvroScoreEnvironment":{
     "input_spec":{
       "table_uri":"kiji://.env/default/users",
       "time_range": { "min_timestamp": 0, "max_timestamp": 9223372036854775807 },
       "columns_to_fields":[{
         "tuple_field_name":"email_address",
         "column": {
           "family": "info",
           "qualifier": "email",
           "max_versions": 1
         }
       }]
     },
     "kv_stores":[],
     "output_spec":{
       "table_uri":"kiji://.env/default/users",
       "output_column": {
         "family": "info",
         "qualifier": "out"
       }
     }
   }
 }
}
</code></pre>

####DummyExtractor.scala

<pre><code>package org.kiji.lifecycle

import org.kiji.modeling.Extractor
import org.kiji.modeling.Scorer
import scala.collection.Seq
import org.kiji.express.flow.FlowCell
import org.apache.avro.util.Utf8

class DummyExtractorScorer extends Extractor with Scorer {
  override val extractFn = extract('email_address -> 'word) { line: Seq[FlowCell[CharSequence]] =>
    line(0).datum.toString()
  }

  override val scoreFn = score('word) { line: String =>
   line.length()
  }
}
</code></pre>




