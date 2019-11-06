import argparse

from s3_factory import S3

if __name__ == "__main__":
    # parse inputs
    parser = argparse.ArgumentParser(description="Upload scripts to S3")
    parser.add_argument("--partition", type=str, help="commercial | china | govcloud", required=True)
    parser.add_argument(
        "--regions",
        type=str,
        help='Valid Regions, can include "all", or comma separated list of regions',
        required=True,
    )
    parser.add_argument(
        "--credential",
        type=str,
        action="append",
        help="STS credential endpoint, in the format <region>,<endpoint>,<ARN>,<externalId>. Could be specified multiple times",
        required=False,
    )
    parser.add_argument("--script", type=str, help="Script to upload", required=True)
    parser.add_argument(
        "--bucket",
        type=str,
        help="Buckets to upload to, defaults to [region]-aws-parallelcluster, comma separated list",
        required=False,
    )
    parser.add_argument(
        "--dryrun", action="store_true", help="Doesn't push anything to S3, just outputs", default=False, required=False
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="If true will over-write existing AWS object",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--rollback", action="store_true", help="Rolls back to previous version.", default=False, required=False,
    )
    parser.add_argument(
        "--versionid", type=str, help="(Optional) Version Id if rolling back.", default=None, required=False
    )
    parser.add_argument(
        "--createifnobucket",
        action="store_true",
        help="Create S3 bucket if it does not exist",
        default=False,
        required=False,
    )
    parser.add_argument(
        "--unsupportedregions", type=str, help="Unsupported regions, comma separated", default="", required=False
    )
    args = parser.parse_args()

    key_path = "scripts"

    s3 = S3(
        bucket=args.bucket,
        script=args.script,
        key_path=key_path,
        dryrun=args.dryrun,
        override=args.override,
        partition=args.partition,
        createifnobucket=args.createifnobucket,
        region=args.regions,
        unsupported_regions=args.unsupportedregions.split(","),
    )

    for region in s3.aws_regions:
        if args.rollback:
            s3.rollback(region, version_id=args.versionid)
        else:
            s3.upload_to_s3(region)
