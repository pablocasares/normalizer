---
title: "Normalizer"
layout: single
toc: false
---

Normalizer is a stream processing engine based on Kafka Streams. Normalizer does maps (stateless and statefull), flatmaps and filters by us. You only need to define a JSON stream where you specify the process logic and how the message are transformed.
It allows you to normalize data streams from different sources to convert them to the same data schema!
Normalizer offers us: scalability, fault tolerance, back-pressure, KV states (RocksDB) and full Kafka integration ... [Try it now!!](https://wizzie-io.github.io/normalizer/getting_started/base_tutorial)

It reads json messages from Apache Kafka and writes them back normalized to Kafka.
 
Some of its abilities are:

* Transform data fields (change values/keys, split fields, replace values and more. Check docs).
* Repartition kafka topics. (Change message keys)
* Filter messages based on conditions.
* Easy scalable (automatic discovering and load balancing).
* Check docs for more!

Normalizer is the one of the components of Wizzie Workflow used by:

* **[Wizzie Data Platform: WDP](https://wizzie.io/what-is-wizzie/#platform)**
* **[Wizzie Community Stack: WCS](https://github.com/wizzie-io/community-stack)**

## Contribute
If you have any idea for an improvement or found a bug, please open an issue. But, if you prefer, you can clone this repo and submit a pull request, helping us make Normalizer a better product.

## License
Normalizer is distributed under Apache 2.0 License.
