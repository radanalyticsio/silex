---
layout: default
title: Silex
---

[silex](https://github.com/radanalyticsio/silex) is a library of reusable code for Spark applications, factored out of applications we've built at Red Hat.  It will grow in the future but for now we have an application skeleton, some added functionality for data frames, and a histogramming RDD.  [Pull requests](https://github.com/radanalyticsio/silex/pulls) and [issue reports](https://github.com/radanalyticsio/silex/issues) are welcome!

You can use Silex [in your own Scala projects](/coordinates/) or [interactively](/interactive).

### Recent news

{% for post in site.posts %}
  <li>
    <span class="post-date">{{ post.date | date: "%b %-d, %Y" }}</span>
    <a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a>
  </li>
{% endfor %}

