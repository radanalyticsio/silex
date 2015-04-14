---
layout: default
title: Silex
---

[silex](https://github.com/willb/silex) is a library of reusable code for Spark applications, factored out of applications we've built at Red Hat.  It will grow in the future but for now we have an application skeleton, some added functionality for data frames, and a histogramming RDD.

### Recent news

{% for post in site.posts %}
  <li>
    <span class="post-date">{{ post.date | date: "%b %-d, %Y" }}</span>
    <a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a>
  </li>
{% endfor %}

