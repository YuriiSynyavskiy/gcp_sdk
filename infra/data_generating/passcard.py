import csv
import random
import uuid
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List

from dotenv import dotenv_values
from google.cloud.storage import Client as StorageClient
from google.oauth2 import service_account

pattern = '%Y%m%d'

START_DATE = datetime.strptime('20000101', pattern)
END_DATE = datetime.strptime('21000101', pattern)
STORAGE_FOLDER = 'passcards'

Passcard = namedtuple(
    'Passcard',
    ['passcard_key', 'person_id', 'security_id', 'start_date', 'expires_at'],
)

config = dotenv_values('.env')


def get_random_date(start: datetime, end: datetime):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )


def get_random_date_range(start: datetime, end: datetime):
    start_date = get_random_date(start, datetime.today())
    end_date = get_random_date(start_date, end)

    return (datetime.strftime(start_date, pattern),
            datetime.strftime(end_date, pattern))


def get_n_random_passcards(n: int):
    passcards = []

    for _ in range(0, n):
        passcards.append(
            Passcard(
                uuid.uuid4(),
                random.randint(1, 10),
                random.randint(1, 10),
                *get_random_date_range(START_DATE, END_DATE),
            )
        )

    return passcards


def write_passcards_to_file(passcards: List[Passcard]) -> str:
    filename = ''.join([
        'passcard_',
        str(datetime.today().replace(microsecond=0).timestamp())[0:-2],
        '.tsv',
    ])

    with open(filename, 'a+') as tsv:
        writer = csv.writer(tsv, delimiter='\t', lineterminator='\n')

        writer.writerow(Passcard._fields)
        writer.writerows(passcards)

    return filename


def get_storage_client() -> StorageClient:
    credentials = service_account.Credentials.from_service_account_file(
        config.get('GOOGLE_APPLICATION_CREDENTIALS'),
    )

    return StorageClient(credentials=credentials)


def upload_passcards_file_to_storage(filename: str):
    storage_client = get_storage_client()

    bucket = storage_client.get_bucket('edu-passage-bucket')
    blob = bucket.blob('/'.join([STORAGE_FOLDER, filename]))
    blob.upload_from_filename(filename)


def run():
    passcards = get_n_random_passcards(100)
    filename = write_passcards_to_file(passcards)

    upload_passcards_file_to_storage(filename)


if __name__ == '__main__':
    run()
