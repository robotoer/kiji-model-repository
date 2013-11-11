Kiji Model Repository ${project.version}
===========================
The Kiji Model Repository allows developers to store and access machine learning models. Using the Kiji Model Repository, data scientists, engineers, analysts can collaborate and publish their artifacts to the model repository for later use by the real-time scoring engine.

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
1. Ensure that the KIJI_CLASSPATH has both the model repository jars and the Kiji Modeling jars mentioned above.
2. Executing kiji model-repo should show:
<pre>
model-repo init - Creates a new model repository.
model-repo deploy - Upload a new version of a lifecycle and/or code artifact.
model-repo upgrade - Upgrades a new model repository.
model-repo delete - Removes existing model repository.
model-repo check - Checks model repository for consistency.
model-repo list - Lists models in model repository.
model-repo update - Updates the production ready flag and sets message for models in the model repository.
</pre>

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
