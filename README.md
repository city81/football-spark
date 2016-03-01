Football Spark
==============

[![Master Build](https://travis-ci.org/city81/football-spark.svg?branch=master)](https://travis-ci.org/city81/football-spark)

This is a project which uses [Scala][scala] and [Spark][spark] to process data files containg football results and odds.


LICENCE
-------

BSD Licence - see LICENCE.txt


TODO
----

1. Process all previous years and divisions
2. Execute queries:
    1. Analyse the results of derbies and their odds 
    2. Analyse the results of short priced away teams 
    3. loads more ...
3. Update to use datasframes and MLs
4. Feed the Spark putput to the [Betfair NG API][betfair] betfair ng api project

[scala]: http://www.scala-lang.org/ "Scala Language"
[spark]: http://spark.apache.org/ "Apache Spark"
[betfair]: https://github.com/city81/betfair-service-ng/ "Betfair NG API"