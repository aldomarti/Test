import boto3, importlib.util, sys, os, tempfile
 
s3 = boto3.client("s3")
 
def lambda_handler(event, context):
    # Prioridad: evento > variable de entorno
    bucket = event.get("script_bucket") or os.environ["SCRIPT_BUCKET"]
    prefix = event.get("script_prefix") or os.environ["SCRIPT_PREFIX"]
    job    = event["job"]
 
    key      = f"{prefix}/{job}.py"
    tmp_path = os.path.join(tempfile.gettempdir(), f"{job}.py")
    s3.download_file(bucket, key, tmp_path)
 
    spec   = importlib.util.spec_from_file_location(job, tmp_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.run(event)