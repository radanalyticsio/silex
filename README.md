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
libraryDependencies += "com.redhat.et" %% "silex" % "0.0.10"
```

Since version 0.0.9, Silex is built for both Scala 2.10 and Scala 2.11.

### Documentation

The [Silex web site](http://silex.freevariable.com/) includes some examples of Silex functionality in use and [API docs](http://silex.freevariable.com/latest/api/#package).

### CI Status

[![Build Status](https://travis-ci.org/willb/silex.svg?branch=develop)](https://travis-ci.org/willb/silex)
[![Coverage Status](https://coveralls.io/repos/github/willb/silex/badge.svg?branch=develop)](https://coveralls.io/github/willb/silex?branch=develop)
