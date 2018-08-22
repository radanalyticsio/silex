---
layout: page
title: Using Silex interactively
permalink: interactive/
---

It's possible to try out Silex interactively without building a project.  You have two options:  you can use `spark-shell` if you have Spark installed, or you can use `sbt console` from a checkout of the source repository.

### Using a Spark shell

You can run Silex under a Spark shell without explicitly downloading it.  Here's the invocation to make it work:

{% highlight bash %}
spark-shell --repositories https://dl.bintray.com/willb/maven/ --packages io.radanalytics:silex_2.11:{{ site.silexVersion }}
{% endhighlight %}

(Thanks to [Jirka Kremser](https://plus.google.com/+JiriKremser) for this suggestion!)

### Using the developer console

If you've checked out the [Silex repository](https://github.com/radanalyticsio/silex), you can run `sbt console` and get an environment with Silex preloaded.  

(In general, you can [customize the behavior of `sbt console`](https://chapeau.freevariable.com/2014/09/interactive-sbt.html).)
