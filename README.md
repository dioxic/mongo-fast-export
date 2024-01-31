# Mongo Fast Export

## Introduction

This tool has been developed by MongoDB Professional Services to provide a fast way to export data from a MongoDB cluster to Json and CSV formats.
It has been designed for performance and will be faster than using the existing, public-available tools (e.g. mongoexport, bsondump).

## Usage

```commandline
Usage: cli [<options>]

Json Mode:
  --json-format=(STRICT|SHELL|EXTENDED|RELAXED)  json format to export (default: RELAXED)

CSV Mode:
  -a, --array=<text>     array field to denormalize
  --date-format=<value>  date format pattern (default: ISO Format)
  --header               include csv header in export
  --delimiter=<text>     delimiter character (default: ,)

Options:
* --uri=<text>             MongoDB connection string
* -d, --database=<text>    database name
* -c, --collection=<text>  collection name
  -q, --query=<value>      filter query (in MQL) (default: {})
  -f, --fields=<text>      fields to include (comma-delimited)
  --projection=<value>     projection (in MQL)
  --mode=(json|csv)        export mode
  -o, --output=<path>      output filename
  --test                   print a single document to the console for testing
  --limit=<int>            limit the number of documents to export
  -h, --help               Show this message and exit
```
## Options

`--array` The array field to denormalize (CSV mode only)

`--date-format` The [date format](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) to use for Bson Date field (CSV mode only).

`--delimiter` The delimiter field (CSV mode only).

`--fields` The fields to include in the output (comma-delimited). The field order is preserved when using CSV export mode.

`--header` Outputs a header row (CSV mode only).

`--help` Returns the cli usage

`--json-format` The [JSON format](https://mongodb.github.io/mongo-java-driver/3.5/javadoc/org/bson/json/JsonMode.html) to use (JSON mode only).

`--limit` Limits the number of documents to retrieve.

`--mode` The export mode (json or csv).

`--output` The export file (defaults to the collection name with the mode extention).

`--projection` An extended [projection](https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/) used for calculated fields (see example). The --fields argument is preferred where possible. 

`--query` The MQL query to apply (this must be written in extended JSON format).

`--test` Prints a single document to the console. Note that when denormalizing an array field in CSV mode this may generate multiple output lines.

`--uri` The [connection string](https://www.mongodb.com/docs/manual/reference/connection-string/). TODO formats are also supported.

## Building from Source

`./gradlew distTar` will create a tar file containing execution scripts, compiled code and all necessary library files.

`./gradlew distZip` will create a zip file containing execution scripts, compiled code and all necessary library files.

These are created under `build\distributions`

## Execution examples

The documents in the target collection have the following structure:
```json
{
  "_id": {
    "$oid": "65ba1775229363274a16d6c3"
  },
  "firstName": "Kale",
  "lastName": "Kovacek",
  "date": {
    "$date": "2016-12-27T19:20:27.442Z"
  },
  "taxlots": [
    {
      "rec": -48802867,
      "cost": -1614471661,
      "type": "type2",
      "code": "D"
    },
    {
      "rec": -1904695773,
      "cost": 477685524,
      "type": "type2",
      "code": "A"
    },
    ...
  ]
}
```

### Basic

```commandline
> bin/mongo-fast-export --uri "mongodb://localhost:27017"  -d myDb -c myCollection --mode json
filter query: {}
connecting to mongodb://localhost:27017...
exporting 'myDb.myCollection' to myCollection.json
exported 0 records
exported 200 records
Completed export of 'myDb.myCollection' in 2.047869500s
```

### Query (JSON)

```commandline
> bin/mongo-fast-export --uri "mongodb://localhost:27017"  -d myDb -c myCollection --mode json --test --fields "_id,firstName" --query "{ 'firstName': 'Graham'}"
filter query: {"firstName": "Graham"}
projection: {"_id": 1, "firstName": 1}
connecting to mongodb://localhost:27017...
exporting 'myDb.myCollection' to console
------------------------------------------
{"_id": {"$oid": "65ba1775229363274a16de82"}, "firstName": "Graham"}
```

### Simple Fields Projection (JSON)
```commandline
> bin/mongo-fast-export --uri "mongodb://localhost:27017"  -d myDb -c myCollection --mode json --test --fields "firstName,lastName,taxlots.code"
filter query: {}
projection: {"_id": 0, "firstName": 1, "lastName": 1, "taxlots.code": 1}
connecting to mongodb://localhost:27017...
exporting 'myDb.myCollection' to console
------------------------------------------
{"firstName": "Kale", "lastName": "Kovacek", "taxlots": [{"code": "D"}, {"code": "A"}, {"code": "E"}]}
```

### Simple Fields Projection (CSV)
```commandline
> bin/mongo-fast-export --uri "mongodb://localhost:27017"  -d myDb -c myCollection --mode csv --test --array taxlots --fields "firstName,lastName,taxlots.code" --header
filter query: {}
projection: {"_id": 0, "firstName": 1, "lastName": 1, "taxlots.code": 1}
connecting to mongodb://localhost:27017...
exporting 'myDb.myCollection' to console
------------------------------------------
firstName,lastName,taxlots.code
Kale,Kovacek,A
Kale,Kovacek,D
Kale,Kovacek,E
```
### Complex Projection (CSV)
```commandline
> bin/mongo-fast-export --uri "mongodb://localhost:27017"  -d myDb -c myCollection --mode csv --test --projection '{ "_id": 1, "fullName": { "$concat": ["$firstName"," ","$lastName"] }}' --header
filter query: {}
projection: {"_id": 1, "fullName": {"$concat": ["$firstName", " ", "$lastName"]}}
connecting to mongodb://localhost:27017...
exporting 'myDb.myCollection' to console
------------------------------------------
_id,fullName
65ba1775229363274a16d6c3,Kale Kovacek
```


## Prerequisites

- Java 17
- Access to the target database

## Limitations

- Only a single array can be denormalized in the CSV mode (using the `--array` argument)

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

| Tool                 | Time  |
|----------------------|-------|
| mongoexport          | 4m12s |
| mongodump*           | 33s   |
| bsondump             | 2m51s |
| mongofastexport json | 53s   |
| mongofastexport csv  | 2m54s |

 `mongodump` is raw BSON format only (not JSON or CSV)