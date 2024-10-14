# NRPD Forecasting Model

This document outlines the formula and components used for calculating the **NRPD Forecast** for a specific channel and increment.

### Formula:
```plaintext
NRPD Forecast for (channel c, increment x) = (NRPD forecast for increment) * (Channel c index) * (Increment x index)
```

### Components:
* NRPD forecast for increment x:
This refers to the NRPD forecast inputs provided by the FPA .

* Indexes:
Both the channel and increment indexes are computed based on data from the last 12 months.

### Index Definitions:
* Channel c index:
This index is calculated as the ratio of the NRPD from trips by new signups from channel c to the NRPD from all trips:
```plaintext
Channel c index = (NRPD from trips by new signups from channel c) / (NRPD from all trips)
```

* Increment x index:
This index is determined by comparing the NRPD from trips associated with increment x to the NRPD from trips associated with increment 1:
```plaintext
Increment x index = (NRPD from trips by increment x) / (NRPD from trips by increment 1)
```

