import csv
import random
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List

from google.cloud.storage import Client as StorageClient

pattern = '%Y%m%d'

START_DATE = datetime.today() - timedelta(days=1)
END_DATE = datetime.strptime('21000101', pattern)
STORAGE_FOLDER = 'passcards'

Passcard = namedtuple(
    'Passcard',
    ['id', 'person_id', 'security_id', 'start_date', 'expires_at'],
)


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

    for i in random.sample(range(1, 101), n):
        passcards.append(
            Passcard(
                i,
                random.randint(1, 10),
                random.randint(1, 10),
                *map(int, get_random_date_range(START_DATE, END_DATE)),
            )
        )

    return passcards


def write_passcards_to_file(passcards: List[Passcard]) -> str:
    filename = ''.join([
        'passcards_',
        str(datetime.today().replace(microsecond=0).timestamp())[0:-2],
        '.csv',
    ])

    with open(filename, 'a+') as _csv:
        writer = csv.writer(_csv, lineterminator='\n')

        writer.writerow(Passcard._fields)
        writer.writerows(passcards)

    return filename


def upload_passcards_file_to_storage(filename: str):
    storage_client = StorageClient()

    bucket = storage_client.get_bucket('passage')
    blob = bucket.blob('/'.join([STORAGE_FOLDER, filename]))
    blob.upload_from_filename(filename)


def run():
    passcards = get_n_random_passcards(random.randint(7, 51))
    filename = write_passcards_to_file(passcards)

    upload_passcards_file_to_storage(filename)


if __name__ == '__main__':
    run()
