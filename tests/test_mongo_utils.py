import io
import re
import shutil
import subprocess
import tarfile
import uuid
import urllib.request

from hypothesis import given, strategies
import pytest
from unittest.mock import MagicMock

from lambda_mongo_utils import mongo_utils


def mock_mongo_file(version, dest):
    with tarfile.open(dest, 'w') as tar:
        for util in mongo_utils.AVAILABLE_MONGO_UTILS:
            cmd = f'echo -n {util} {version}'.encode('ascii')
            tar_info = tarfile.TarInfo(f'mongo-xxx/bin/{util}')
            tar_info.size = len(cmd)

            tar.addfile(tar_info, io.BytesIO(cmd))


@given(
    utils=strategies.lists(
        strategies.sampled_from(mongo_utils.AVAILABLE_MONGO_UTILS),
        unique=True,
    ),
)
def test_download_utils(tmp_path, monkeypatch, utils):

    versions = {
        '4.2-latest': 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v4.2-latest.tgz',  # noqa: E501
        '4.0-latest': 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v4.0-latest.tgz',  # noqa: E501
        '4.2.0': 'http://downloads.mongodb.org/linux/mongodb-linux-x86_64-amazon2-v4.2.0.tgz',            # noqa: E501
    }

    for version, version_url in versions.items():
        mock = MagicMock()
        monkeypatch.setattr(urllib.request, 'urlretrieve', lambda url, dest: (
            mock_mongo_file(version, dest), mock(url),
        ))
        dest = tmp_path / 'tmp-download-utils' / version
        shutil.rmtree(dest, ignore_errors=True)

        r = mongo_utils.download_utils(dest=dest, version=version, utils=utils)
        mock.assert_called_once_with(version_url)

        util_paths = dest.glob('*')
        assert set(u.name for u in util_paths) == set(utils)

        for util, util_path in r.items():
            process = subprocess.run(
                ['sh', '-c', util_path],
                capture_output=True,
                check=True,
                text=True,
            )
            assert f'{util} {version}' == process.stdout

        shutil.rmtree(dest, ignore_errors=True)
        with pytest.raises(ValueError):
            mongo_utils.download_utils(
                dest=dest, version=version, utils=['non-existing-util'])

        dest = dest / f'binary--{uuid.uuid4()}'
        with dest.open('w') as fp:
            fp.write('testing...')

        with pytest.raises(Exception) as e:
            mongo_utils.download_utils(dest=dest, version=version)

        assert re.search('not a directory', str(e.value))
