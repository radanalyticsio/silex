# silex

something to help you spark

This is a library of reusable code for Spark applications, factored out of applications we've built at Red Hat.  It will grow in the future but for now we have an application skeleton, a simple natural join for data frames, and a histogramming RDD.

### Using silex

Add the following resolver to your project:

```scala
resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/"
```

and then add Silex as a dependency:

```scala
libraryDependencies += "com.redhat.et" %% "silex" % "0.0.8"
```

(or a later version, if we've released one!)

### Documentation

The [Silex web site](http://silex.freevariable.com/) includes some examples of Silex functionality in use and [API docs](http://silex.freevariable.com/latest/api/#package).

### CI Status

[![Build Status](https://travis-ci.org/willb/silex.svg?branch=develop)](https://travis-ci.org/willb/silex)
[![Coverage Status](https://coveralls.io/repos/github/willb/silex/badge.svg?branch=develop)](https://coveralls.io/github/willb/silex?branch=develop)
