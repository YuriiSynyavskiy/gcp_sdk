from google.cloud.logging_v2 import Client


def get_logger(logger_name):
    logging_client = Client()

    return logging_client.logger(logger_name)
