# High Throughput Kafka Processing System

## Overview
<font size="3">Designed and implemented a scalable Kafka-based system capable of processing **10 million messages efficiently** using async Consumers and Optimized Batching.</font>
- An initial request from the client hits the /stockValue API. 
- The service generates data and publishes it to Kafka. 
- The system is currently capable of **processing ~10 million records per run.**

## Performance Optimization Results:
- **Initial:** 224 sec for 100M records (~446K msg/sec)
- **After tuning:** 204 sec (~490K msg/sec)
- **Final optimized:** 167 sec (~598K msg/sec)

## Overall improvement:
- **~25% reduction** in processing time
- **~34% increase** in throughput

## Optimizations Applied:
- Increased **batch size and linger.ms** for better batching.
- Enabled **compression** to reduce network and disk overhead.
- Converted producer flow to fully async.
- Tuned thread pool size for **parallel processing.**

## Results:
- **Time reduced:** 224s → 167s (~25% improvement)
- **Throughput increased:** ~446K → ~598K msg/sec (~34% improvement)

