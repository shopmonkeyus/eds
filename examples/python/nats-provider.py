import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import NatsError

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
    reply = msg.reply
    data = msg.data.decode()
    try:
        # Add your message handling logic here
        logger.info(f"Received a message on '{subject} {reply}': {data}")

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
    

if __name__ == '__main__':
    # Initialize logger and other resources
    logger = ConsoleLogger()  # Initialize your logger 
    # Run the main function
    asyncio.run(main( logger))