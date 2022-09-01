#### User requirements

A summary of user groups can be found [here](https://docs.google.com/document/d/1vF0fT7Dg2AkpaUl2OpPZrqDDo_fCGqFR3G8vRKpZsps/edit?usp=sharing).

* Run QC automatically when new data arrives
* Run QC on demand for testing, and integrating other systems
* Able to easily write / integrate new QC code 
* Give understandable quality flags back from QC
* Keep enough information from QC to be able to meet different user groups needs
* Keep enough information to determine _provenance_


#### Non functional requirements 

 A document with discussion related to architecture and non functional requirements can be found [here](https://docs.google.com/document/d/1vLO5OtfMrkI9vwPSWCevlwZBD5CpqQJh7JtDyrOsXPw/edit?usp=sharing)

_If we approximately double current number of stations and increase the resolution to minute data we would have about 2 million observations every hour, or 35,000 every minute. However, we can assume we only need to run basic tests (dip, freeze...) on new data and can wait until larger data sets have accumulated to run more compute intensive spatial tests._

we can potentially prioritize running tests based on 2 things: 
1. hierarchy of stations (more important first)
2. prioritized tests (hierarchy of tests?)

* **Scalability:** Able to meet current/future needs (particularly when larger ammounts of data come in at particular times). Do not send large amounts of data around over the network, to reduce the likelyhood of this causing a bottleneck. 
* **Availability:** Run QC in "real time" (at least minimize wait for flags for the most prioritized / basic QC) WMO standard is < 5mins
* **Robustness:** Able to work if one datarom is down, but may need to priotize certain data and allow the rest to be run more slowly
* **Maintainability:** Simple as possible architecture / deployment, while still able to adapt to future changes if needed
* **Performance:** Efficient code for the core features / QC (how efficient this needs to be is somewhat dependent on the actual number of observations the system needs to be able to QC and over what time, as well as how they will be processed)
* **Measurability:** Prometheus metrics for monitoring
