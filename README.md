# Mongo Fast Export

## Introduction

This tool has been developed by MongoDB Professional Services to provide a fast way to export data from a MongoDB cluster to Json and CSV formats.
It has been designed for performance and will be faster than using the existing, public-available tools (e.g. mongoexport, bsondump).

## Prerequisites

- Java 17
- Access to the target database

## Testing

Performance testing has been conducted with the following setup:

| Component | Value |
| ---- | ---- |
| Atlas Tier | M30 |
| Client | m4.4xlarge (16 vCPU) |
| Data Volume | 2.31GB uncompressed |
| Document Size | 230 bytes |
| Document Count | 10 Million |

Results:

| Tool | Time |
| ---- | ---- |
| mongoexport | 4m12s |
| mongodump | 33s |
| bsondump | 2m51s |
| mongofastexport json | 53s |
| mongofastexport csv | 3m38s |