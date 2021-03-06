

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"


//tag::addSparkPackagesPlugin[]
resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")
//end::addSparkPackagesPlugin[]

//addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

//tag::sbtJNIPlugin[]
//addSbtPlugin("ch.jodersky" %% "sbt-jni" % "1.2.6")
//end::sbtJNIPlugin[]

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
