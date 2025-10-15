import watchtower
import boto3

session = boto3.Session()
handler = watchtower.CloudWatchLogHandler(
    boto3_session=session,
    log_group="refinement-agent-logs",
    stream_name="local-instance"
)
logger.addHandler(handler)
