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
            eds_input_raw = input()
            # Process the input (you can add your own logic here)
            eds_input = json.loads(eds_input_raw)
            input_data = eds_input.get("data")
            sys.stdout.write(f"[EDS Echo Python] data: {json.dumps(input_data, indent=4)}'\n")
        except Exception as err:
             sys.stdout.write(f"\n[EDS Echo Python] error:{err}")
        except KeyboardInterrupt:
            sys.stdout.write("\n[EDS Echo Python] Shutting down...")
            break

if __name__ == "__main__":
    read_stream_from_stdin()