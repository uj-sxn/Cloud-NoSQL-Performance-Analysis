# Comparative Analysis of Cloud-Based NoSQL Databases

This project evaluates the performance of three managed NoSQL databases—**Amazon DynamoDB**, **MongoDB Atlas**, and **Apache Cassandra (Astra DB)**—using an e-commerce dataset of ~51,000 records. We performed empirical testing to determine how their unique architectures impact latency and throughput across CRUD and bulk operations.

### Performance Results:

No single database dominated all categories, confirming that technology selection depends on specific workload requirements:

*Amazon DynamoDB*: Fastest for **single-item writes** (291.91 ms), **range scans** (376.97 ms), and **deletions** (38.37 ms).

*MongoDB Atlas*: The "champion" for **bulk data loading** (939.11 ms), handling high-volume ingestion most efficiently.

*Astra DB (Cassandra)*: Demonstrated the best performance for **specific ID lookups** (382.06 ms).


### Key Takeaways:

*DynamoDB* is ideal for real-time transactional hotspots like carts and sessions.

*MongoDB Atlas* excels in flexible querying and complex analytical workloads.

*Astra DB* is preferred for write-intensive, globally distributed scenarios such as IoT telemetry.
