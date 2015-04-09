---
layout: page
title: Ivy coordinates
permalink: /coordinates/
---

To use Silex in your own projects, add the following library dependency to your build.sbt:

```scala
resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/"
```

and then add Silex as a dependency:

```scala
libraryDependencies += "com.redhat.et" %% "silex" % "{{ site.silexVersion }}"
```
