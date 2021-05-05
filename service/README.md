## Trance Web Service

### Setup

Requires trance jar is in base directory called `lib`. Follow these steps from `service` dir:

```
mkdir lib
cd ../compiler
sbt package
cp target/scala-2.12/framework_2.12-0.1.jar ../service/lib/
```

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
