# silex

something to help you spark

This is a library of reusable code for Spark applications, factored out of applications we've built at Red Hat.  It will grow in the future but for now we have an application skeleton, some useful extensions to data frames and RDDs, utility functions for handling time and stemming text, and helpers for feature extraction.

### Using silex

Add the following resolver to your project:

```scala
resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/"
```

and then add Silex as a dependency:

```scala
libraryDependencies += "io.radanalytics" %% "silex" % "0.1.2"
```

Since version 0.0.9, Silex is built for both Scala 2.10 and Scala 2.11.  Since version 0.1.0, Silex depends on Spark 2.0.

### Documentation

The [Silex web site](http://silex.freevariable.com/) includes some examples of Silex functionality in use and [API docs](http://silex.freevariable.com/latest/api/#package).

### Notes for developers

To cut a new release, use the `git flow` release workflow.

1.  Start a new release branch with `git flow release start x.y.z`
2.  Incorporate any release-specific patches that do not belong on the `develop` branch
3.  Bump version numbers in the [README](README.md), [build definition](build.sbt), and [Jekyll configuration](src/jekyll/_config.yml).
4.  Run tests for every cross build:  `sbt +test`
5.  Publish binary artifacts to bintray for each cross-build:  `sbt +publish`
6.  Publish an updated site for the project:  `sbt ghpages-push-site`


### CI Status

[![Build Status](https://travis-ci.org/radanalyticsio/silex.svg?branch=develop)](https://travis-ci.org/radanalyticsio/silex)
[![Coverage Status](https://coveralls.io/repos/github/willb/silex/badge.svg?branch=develop)](https://coveralls.io/github/willb/silex?branch=develop)
