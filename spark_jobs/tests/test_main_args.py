from main import _parse_runtime_args


def test_parse_config_path_and_pipeline_root():
    args = _parse_runtime_args(
        ["--config-path", "s3://bucket/glue/config/configs.yaml", "--unused", "x"]
    )
    assert args["config-path"] == "s3://bucket/glue/config/configs.yaml"
    assert "pipeline-root" not in args


def test_parse_equals_form():
    args = _parse_runtime_args(["--pipeline-root=s3://bucket/lakehouse", "--flag-only"])
    assert args["pipeline-root"] == "s3://bucket/lakehouse"
    assert args["flag-only"] == "true"
