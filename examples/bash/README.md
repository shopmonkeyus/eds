## EDS Bash Example

This example shows how to run `eds-server` with a Bash target.

Any executable file can be invoked using the `'file:///<path-to-executable>'` extra arg as part of the `eds-server` cli command.

```bash
#!/bin/bash

while read INPUT; do
  echo $INPUT | jq
done
```

`echo.sh` is a very basic program that simply listens to STDIN and writes the input to STDOUT.

This can easily extended to integrate with whichever downstream systems it needs too, in addition to being able to load whichever custom tools, languages, etc.

The file-based `eds-server` plugins do not have alot of the batteries-included like the `postgresql` plugin, but they do allow for nearly infinite customization and deep integration with your own systems. 

`eds-server` will stream JSON lines to STDIN. The JSON will have the following shape:
```json
{
    "data": {...},
    "schema": {...}
}
``` 
The `data` element is the changefeed record which includes information about which fields changed and if the operation is an create, update or delete.

The `schema` element contains the schema of the data (usually contained in the `before` or `after` data element, depending on the operation.) This can be used to materialize the data along with its schema, if that is desired.
---
## Execution

- Open a terminal in this directory (`./examples/bash`)
- Run:
    ```bash
    eds-server server --creds <path-to-your-eds-creds>.creds 'file:///<absolute path to>/echo.sh' --verbose --consumer-prefix testing
    ```