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

_If we approximately double current number of stations and increase the resolution to minute data we would have about 2 million observations every hour, or 35,000 every minute._

* **Scalability:** Able to meet current/future needs (particularly when larger ammounts of data come in at particular times)
* **Availability:** Run QC in "real time" (at least minimize wait for flags for the most prioritized / basic QC) WMO standard is < 5mins
* **Robustness:** Able to work if one datarom is down, but may be slower due to less resources and / or need to prioritize certain data
* **Maintainability:** Simple as possible architecture / deployment, while still able to adapt to future changes if needed
* **Performance:** Efficient code for the core features / QC
* **Measurability:** Prometheus metrics for monitoring
