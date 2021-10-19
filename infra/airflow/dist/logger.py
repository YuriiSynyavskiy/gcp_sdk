from google.cloud import logging

def get_logger(log_name):
    logging_client = logging.Client()
    return logging_client.logger(log_name)


