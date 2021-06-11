## Trance Web Service

### Setup

Requires trance jar is in base directory called `lib`. Follow these steps from `service` dir:

```
mkdir lib
cd ../compiler
sbt package
cp target/scala-2.12/framework_2.12-0.1.jar ../service/lib/

cd ../executor/spark
sbt package 
cp target/scala-2.12/sparkutils_2.12-0.1.jar ../../service/lib
```
Add these jars to the lib folder to the External libraries found on your favorite 
IDE project settings.

### Example requests

#### POST /nrcode:

Without an assignment:
```
{
  "_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "title": "TestQuery",
  "body": "for o in occurrences union {(sid := o.donorId)}"
}
```

With an assignment:
```
{
  "_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "title": "TestQuery",
  "body": "TestQuery <= for o in occurrences union {(sid := o.donorId)}"
}
```

Nested test:
```
{
  "_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "title": "NestTest",
  "body": "NestTest <= for o in occurrences union {(sid := o.donorId, cons := for t in o.transcript_consequences union {(gene := t.gene_id)})}"
}
```
