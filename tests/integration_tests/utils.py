from disdat import api


class Config:
    EXECUTION_ROLE = 'arn:aws:iam::531568561199:role/emr-chapi-lambda'
    REGION = 'us-east-1'
    CONTEXT = 'simple_dev_pipeline'
    S3_URL = 's3://step-function-cache-bucket'
    LAMBDA_WORKER_NAME = 'cache_lambda'


class VersionChecker:

    def __init__(self, context: str, bundle_names: list):
        self.context = context
        self.monitor_bds = bundle_names
        api.context(context_name=context)
        api.remote(local_context=context, remote_context=context, remote_url=Config.S3_URL)
        self.version = {}
        for bd in self.monitor_bds:
            self.version[bd] = self.capture_version(bd)

    def validate_execution(self, bd: str, expected_version_gap: int = 1):
        if len(self.version[bd]) == 0:
            assert expected_version_gap > 0, 'new previous data found, gap must be greater than 0'
            return
        versions = self.capture_version(bd)
        recorded_latest = self.version[bd][0]
        idx = 0
        found = False
        for version in versions:
            if recorded_latest == version:
                found = True
                break
            idx += 1
        # print(versions, self.version[bd])
        if found is False:
            idx = -1
        assert idx == expected_version_gap, 'expected gap = {}, actual gap = {}'.format(expected_version_gap, idx)

    def capture_version(self, bundle: str) -> list:
        api.pull(self.context, bundle_name=bundle, localize=False)
        bundles = api.search(self.context, bundle)
        return [(b.uuid, b.creation_date) for b in bundles]