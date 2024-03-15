import asyncio
import os
import jwt
import sys
import re
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

async def subscribe_to_messages(logger, js, company_id):
    subject = f"dbchange.*.*.{company_id}.*.PUBLIC.>"
    durable_name = "durable-" + company_id  # Define a durable name for the subscription
    try:
        # Subscribe to the subject with the message handler
        await js.subscribe(subject,  cb=lambda msg: message_handler_wrapper(logger, msg), durable=durable_name)
        logger.info(f"Subscribed to subject {subject}")
    except NatsError as e:
        logger.error(f"Error subscribing to subject {subject}: {e}")

async def main(creds, logger):
    # Initialize NATS client
    nc = NATS()
    js = nc.jetstream()
    # Connect to local NATS server
    await nc.connect(servers=["nats://0.0.0.0:4223"])

    # List of company IDs - this could be grabbed from the creds file, or manually entered?
    company_ids = getCompanyIds(creds, logger)

    # Start subscribing to messages for each company ID
    for company_id in company_ids:
        
        await subscribe_to_messages(logger, js, company_id)
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
    
def getCompanyIds(creds, logger): 

    if creds != "":
        if not os.path.exists(creds):
            logger.error("error: invalid credential file: %s", creds)
            sys.exit()

        try:
            with open(creds, "rb") as file:
                content = file.read()
        except Exception as e:
            logger.error(f"error: reading credentials file: {str(e)}")
            sys.exit()
        # Use regular expressions to extract the JWT token substring
        pattern = rb"-----BEGIN NATS USER JWT-----(.*?)------END NATS USER JWT------"
        match = re.search(pattern, content, re.DOTALL)

        if match:
            jwt_token_bytes = match.group(1)  # Extract the captured group
            jwt_token = jwt_token_bytes.decode("utf-8").strip()
        else:
            logger.error("error: JWT token not found in file")
            sys.exit()
        try:
            claim = jwt.decode(jwt_token, options={"verify_signature": False})
        except jwt.exceptions.DecodeError as e:
            logger.error(f"error: decoding JWT: {str(e)}")
            sys.exit()
        except Exception as e:
            logger.error(f"error: parsing valid JWT: {str(e)}")
            sys.exit()

        companyIDs = claim["aud"].split(",")
        if len(companyIDs) == 0:
            logger.error(f"error: invalid JWT claim. missing audience")
            sys.exit()
        return companyIDs

if __name__ == '__main__':
    # Initialize logger and other resources
    creds = "../add-your/creds-file-here/credentialfile.creds"
    logger = ConsoleLogger()  # Initialize your logger 
    # Run the main function
    asyncio.run(main(creds, logger))