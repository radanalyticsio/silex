---
layout: page
title: Ivy coordinates
permalink: coordinates/
---

To use Silex in your own projects, add the following resolver to your `build.sbt`:

{% highlight scala %}
resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/"
{% endhighlight %}

and then add Silex as a dependency:

{% highlight scala %}
libraryDependencies += "com.redhat.et" %% "silex" % "{{ site.silexVersion }}"
{% endhighlight %}
