import logging

def config_logger(name):
    # Create a logger
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.propagate = False

    # Set the logging level
    logger.setLevel(logging.INFO)

    # Create a Console handler with the logging level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a loger time formatter
    formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S %Z')
    # Set the formatter to the console handler
    console_handler.setFormatter(formatter)
    # Add the logger to the console handler
    logger.addHandler(console_handler)

    return logger