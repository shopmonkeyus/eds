import asyncio
import nats
import atexit
from nats.errors import TimeoutError
from functools import wraps
import typer


def typer_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper

def exit_handler(nc): 
    nc.close()   

@typer_async
async def main(nats_host: str = typer.Argument("nats://localhost:4223", envvar="SM_EDS_NATS_HOSTS"),
               stream: str = typer.Argument("dbchange.vehicle.UPDATE.>", envvar="SM_EDS_NATS_SUBJECT"),
               consumer_group: str = typer.Argument("vehicle_create", envvar="SM_EDS_NATS_CONSUMER_GROUP"),
               ):
    
    print("Connecting to nats")
    nc = await nats.connect(nats_host)
    atexit.register(exit_handler, nc = nc)
    
    print("Connected to nats: %s", nc.servers)
    js = nc.jetstream()

    async def cb(msg):
        try:
            print(msg)
            await msg.ack()
        except:
            pass
        
    sub = await js.subscribe(stream, durable=consumer_group, ordered_consumer=True, manual_ack=True, cb=cb)
    data = bytearray()
    
    while True:
        try:
            await sub.next_msg()
        except:
            continue
            
    
    

if __name__ == '__main__':
    typer.run(main)