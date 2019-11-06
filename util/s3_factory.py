import logging

import boto3
from botocore.client import ClientError

from pcluster.utils import error

LOGGER = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(module)s - %(message)s", level=logging.INFO)


class S3:
    """
    Class to manage S3 Operations.
    """

    def __init__(
        self,
        bucket,
        script,
        key_path,
        dryrun,
        override,
        partition,
        createifnobucket,
        region,
        unsupported_regions,
        aws_credentials=None,
    ):
        """

        :param buckets: buckets to upload too.
        :param partition: partition to upload into.
        :param region: comma separated list of regions or "all"
        :param unsupported_regions: regions not supported.
        :param aws_credentials: AWS Credentials
        """
        self.partition = partition
        self.script = script
        self.key_path = key_path
        self.dryrun = dryrun
        self.override = override
        self.main_region = self.get_main_region()
        self.bucket = bucket
        self.aws_credentials = aws_credentials
        self.s3_client = self.get_s3_client()
        self.s3_resource = self.get_s3_resource()
        self.createifnobucket = createifnobucket

        if region == "all":
            aws_regions = self.get_all_aws_regions()
        else:
            aws_regions = region.split(",")
        self.aws_regions = set(aws_regions) - set(unsupported_regions)

    def get_bucket(self, bucket, region=None):
        """
        Returns bucket to upload too.

        :param bucket: custom bucket to use
        :return: bucket to upload too
        """
        bucket = bucket if bucket else "%s-aws-parallelcluster" % region

        nobucket = False
        try:
            self.s3_resource.meta.client.head_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                nobucket = True

        if nobucket and self.createifnobucket:
            LOGGER.info("Creating bucket s3://{0}".format(bucket))
            self.create_bucket(bucket, region)

        return bucket

    def get_main_region(self):
        """
        Get main region given the partition.

        :return: main region
        """
        if self.partition == "commercial":
            return "us-east-1"
        elif self.partition == "govcloud":
            return "us-gov-west-1"
        elif self.partition == "china":
            return "cn-north-1"
        else:
            error("Unsupported Region", fail_on_error=True)

    def get_all_aws_regions(self):
        """
        Get a list of all aws regions in a partition.

        :return: list of aws regions
        """
        ec2 = boto3.client("ec2", region_name=self.main_region)
        return set(r.get("RegionName") for r in ec2.describe_regions().get("Regions"))

    def get_s3_client(self):
        """
        Get boto3 s3 client

        :return: s3 client
        """
        if self.aws_credentials:
            return boto3.client(
                "s3",
                region_name=self.main_region,
                aws_access_key_id=self.aws_credentials.get("AccessKeyId"),
                aws_secret_access_key=self.aws_credentials.get("SecretAccessKey"),
                aws_session_token=self.aws_credentials.get("SessionToken"),
            )
        else:
            return boto3.client("s3", region_name=self.main_region)

    def get_s3_resource(self):
        """
        Gets s3 resource which is useful for operations such as head_bucket()

        :return: s3 resource
        """
        return boto3.resource("s3", region_name=self.main_region)

    def put_object(self, bucket, key, data, script):
        try:
            response = self.s3_client.put_object(Bucket=bucket, Key=key, Body=data, ACL="public-read")
            if response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
                LOGGER.info("Successfully uploaded %s to s3://%s/%s" % (script, bucket, key))
        except ClientError as e:
            error("Couldn't upload %s to bucket s3://%s/%s" % (script, bucket, key), fail_on_error=False)
            if e.response["Error"]["Code"] == "NoSuchBucket":
                error("Bucket is not present.", fail_on_error=True)
            else:
                raise e
            pass

    def create_bucket(self, bucket, region):
        """
        Create a Bucket.

        :return: bucket object
        """
        LOGGER.info("No bucket, creating now: ")
        if self.main_region == "us-east-1":
            self.s3_client.create_bucket(Bucket=bucket)
            self.s3_client.get_waiter("bucket_exists").wait(Bucket=bucket)
        else:
            self.s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
        self.s3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        LOGGER.info("Created %s bucket. Bucket versioning is enabled, please enable bucket logging manually." % bucket)
        return bucket

    def upload_to_s3(self, region):
        """
        Upload file to S3, if it exists will only upload if override = true.

        :param region: region to upload too.
        :return: None
        """

        key = "%s/%s" % (self.key_path, self.script)
        bucket = self.get_bucket(self.bucket, region)
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            LOGGER.warning("Warning: %s already exist in bucket %s" % (key, bucket))
            exist = True
        except ClientError:
            exist = False
            pass

        if (exist and self.override and not self.dryrun) or (not exist and not self.dryrun):
            with open(self.script, "rb") as data:
                self.put_object(bucket, key, data, self.script)
        else:
            error(
                "Not uploading %s to bucket %s, object exists %s, override is %s, dryrun is %s"
                % (self.script, bucket, exist, self.override, self.dryrun),
                fail_on_error=True,
            )

    def rollback(self, region, version_id=None):
        """
        Rolls back to the last version.

        :param region: aws region
        :param version_id: if you desire to rollback to a previous version, specify version_id
        :param is_public: sets ACL = public-read
        :return: true if successful
        """
        key = "%s/%s" % (self.key_path, self.script)
        bucket = self.get_bucket(self.bucket, region)

        # Get last version, if no version_id is provided
        if version_id is None:
            try:
                LOGGER.info("Getting previous version of s3://{0}/{1}".format(bucket, key))
                self.s3_client.head_object(Bucket=bucket, Key=key)
                versions = self.s3_client.list_object_versions(Bucket=bucket, Prefix=key, MaxKeys=3).get("Versions")
                if len(versions) < 2:
                    error("Not enough versions found for object s3://%s/%s" % (bucket, versions), fail_on_error=True)
                version_id = versions[1].get("VersionId")
                date = versions[1].get("LastModified")
                print("Found Version {0} from {1}".format(version_id, date))
            except ClientError:
                error("No such object s3://%s/%s" % (bucket, key), fail_on_error=True)

        # Get object from previous version and upload if not a dryrun
        try:
            local_file = "s3://{bucket}/{key}#{version_id}".format(bucket=bucket, key=key, version_id=version_id)
            LOGGER.info("Getting Object {0}".format(local_file))
            res = self.s3_client.get_object(Bucket=bucket, Key=key, VersionId=version_id)
            if res.get("ResponseMetadata").get("HTTPStatusCode") == 200:
                data = res.get("Body").read()
                LOGGER.info("Found Object with Content {0}".format(data))
                if not self.dryrun:
                    LOGGER.info("Uploading Object {0}".format(local_file))
                    self.put_object(bucket=bucket, key=key, data=data, script=local_file)
                else:
                    LOGGER.warning(
                        "Not restoring {local_file} with content \n {data}".format(local_file=local_file, data=data)
                    )
        except ClientError:
            error("No such object s3://%s/%s" % (bucket, key), fail_on_error=True)
