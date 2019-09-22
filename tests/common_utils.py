import logging
import os
import time

from faker import Faker

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv('LOG_LEVEL', 'INFO'))


def wait_for_mongo_to_be_up(container, timeout=None):
    '''
    Waits until I can connect to Mongo inside the given container.
    '''
    rc = None
    timeout = timeout or 10
    t0 = time.time()
    stderr = ''

    while time.time() - t0 < timeout:
        # Try again until Mongo connects
        rc, (_, stderr) = container.exec_run(
            ['mongo', '--eval', 'quit()'],
            demux=True,
        )
        if rc == 0:
            break
        else:
            time.sleep(1.0)

    if rc != 0 and stderr:
        LOGGER.warning(stderr.decode('utf-8'))

    return rc == 0


def fake_docs(n=1000):
    '''
    Generates fake documents.
    '''
    faker = Faker()

    return [
        {
            'name': faker.name(),
            'address': faker.address(),
        }
        for i in range(n)
    ]
