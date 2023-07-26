## EDS Python Example

This example shows how to run `eds-server` with a Python target.

Any executable file can be invoked using the `'file:///<path-to-executable>'` extra arg as part of the `eds-server` cli command.

For python, we'll add a wrapper bash script, `echo-py-driver.sh` that will invoke `python echo.py`.
```bash
#!/bin/bash

python3 echo.py
```

`echo.py` is a very basic program that simply listens to STDIN and writes the input to STDOUT.

This can easily extended to integrate with whichever downstream systems it needs too, in addition to being able to load whichever custom python modules (pandas, requests, etc).

The file-based `eds-server` plugins do not have alot of the batteries-included like the `postgresql` plugin, but they do allow for nearly infinite customization and deep integration with your own systems. 

```python
import signal
import sys
import json
def handle_sig(signum, frame):
    print("\n[EDS Echo Python] Received SIGINT. Exiting the program.")
    exit(0)

def read_stream_from_stdin():
    sys.stdout.write("[EDS Echo Python] example is ready for input!")
    # Register the signal handler for SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, handle_sig)

    while True:
        try:
            # Read a line of input from STDIN
            eds_input = input()
            # Process the input (you can add your own logic here)
            sys.stdout.write(f"[EDS Echo Python] Received input: {eds_input} + '\n")

        except KeyboardInterrupt:
            sys.stdout.write("\n[EDS Echo Python] Shutting down...")
            break

if __name__ == "__main__":
    read_stream_from_stdin()
```

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

- Open a terminal in this directory (`./examples/python`)
- Run:
    ```bash
    eds-server server --creds <path-to-your-eds-creds>.creds 'file:///<absolute path to>/echo-py-driver.sh' --verbose --consumer-prefix testing
    ```