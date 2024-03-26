import aiohttp
import asyncio
import sys
from nats.aio.client import Client as NATS
from nats.aio.errors import NatsError


HEALTH_CHECK_FREQUENCY_IN_SECONDS = 60  # seconds
HEALTH_CHECK_URL =  "http://localhost:8080/status/health"
class ConsoleLogger:
    @staticmethod
    def error(message):
        print(f"ERROR: {message}")

    @staticmethod
    def info(message):
        print(f"INFO: {message}")

    @staticmethod
    def debug(message):
        print(f"DEBUG: {message}")

async def message_handler_wrapper(logger, msg):
    # This lets us add the logger to our callback function
    await message_handler(logger, msg)

async def message_handler(logger, msg):
    subject = msg.subject
    headers = msg.headers
    reply = msg.reply
    data = msg.data.decode()
    try:
        # Add your message handling logic here
        logger.info(f"Received a message on '{subject} {reply}': {data}")
        logger.info(f"Received headers: {headers}")
        await msg.ack()  # Acknowledge the message to NATS so it doesn't get redelivered
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await msg.nack()

    pass

async def subscribe_to_messages(logger, js):
    subject = f"dbchange.>"
    durable_name = "durable-nats-consumer" 
    try:
        # Subscribe to the subject with the message handler
        await js.subscribe(subject,  cb=lambda msg: message_handler_wrapper(logger, msg), durable=durable_name)
        logger.info(f"Subscribed to subject {subject}")
    except NatsError as e:
        logger.error(f"Error subscribing to subject {subject}: {e}")

async def main(logger):
    # Initialize NATS client
    nc = NATS()
    js = nc.jetstream()
    # Connect to local NATS server
    await nc.connect(servers=["nats://0.0.0.0:4223"])

    await subscribe_to_messages(logger, js)
        
        
    # Wait for the program to exit
    await asyncio.create_task(wait_for_exit(logger, nc))

async def wait_for_exit(logger, nc):
    # Wait for KeyboardInterrupt or cancellation
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Disconnecting from NATS server.")
        await nc.close()    
    except asyncio.CancelledError:
        logger.info("Received cancellation. Disconnecting from NATS server.")
        await nc.close()
    except Exception as e:
        logger.error(f"Error waiting for exit: {e}")
        await nc.close()
    

async def healthcheck(logger):
    timeout = 10 

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                response = await asyncio.wait_for(session.get(HEALTH_CHECK_URL), timeout=timeout)

                if response.status == 200:
                    logger.debug("Health check successful")
                else:
                    logger.error(f"Health check failed with status code: {response.status}")
                    sys.exit(1)

        except asyncio.TimeoutError:
            logger.debug("Health check timed out")
            sys.exit(1)
        except aiohttp.ClientError as e:
            logger.debug(f"Health check failed: {e}")
            sys.exit(1)

        await asyncio.sleep(HEALTH_CHECK_FREQUENCY_IN_SECONDS) 

async def run_tasks():
    # Initialize logger and other resources
    logger = ConsoleLogger()  # Initialize your logger 

    # Run the main function
    task_healthcheck = asyncio.create_task(healthcheck(logger))
    task_main = asyncio.create_task(main(logger))

    # Run the main function in a separate task
    await asyncio.gather(task_main, task_healthcheck)

    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Exiting the program.")
        pass
    except asyncio.CancelledError:
        logger.info("Received cancellation. Exiting the program.")
        pass
    except Exception as e:
        logger.error(f"Error running tasks: {e}")
        sys.exit(1)

if __name__ == '__main__':

    # Run the tasks within the event loop
    try:
        asyncio.run(run_tasks())
    except KeyboardInterrupt:
        pass