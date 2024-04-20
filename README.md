# Influxdb renamer

Just a little program to rename the values of a tag in influxdb.
Currently it does not batch queries as I was missing data. Therefore it is really slow, but works :)


```
Usage: influxdb_renamer [OPTIONS] --host <HOST> --token <TOKEN> --bucket <BUCKET> --measurement <MEASUREMENT> --tag <TAG> --old-name <OLD_NAME> --new-name <NEW_NAME>

Options:
      --host <HOST>                Host to connect to. E.g. http://localhost:8086
      --token <TOKEN>              Access token
  -b, --bucket <BUCKET>            Bucket the target is in
  -m, --measurement <MEASUREMENT>  The measurement to use
      --tag <TAG>                  The tag to use
  -o, --old-name <OLD_NAME>        The old value
  -n, --new-name <NEW_NAME>        The new value
      --batch-size <BATCH_SIZE>    Number of queries to batch (currently ignored) [default: 1000]
  -h, --help                       Print help
  -V, --version                    Print version
```
