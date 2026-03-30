import yaml


def load_config(config_path):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    return config


def get_path(config, *keys):
    root = config["paths"]["root"]

    path = config["paths"]

    for key in keys:
        path = path[key]

    return f"{root}/{path}"
