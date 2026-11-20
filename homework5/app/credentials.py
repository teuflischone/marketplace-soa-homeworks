import json
from pathlib import Path
from secrets import token_hex

POSTGRES_PASSWORD = Path("/run/postgres-access/password")
S3_CONFIG = Path("/run/s3-access/identity.json")


def write_once(path, value):
    if path.exists():
        return
    staging = path.with_suffix(".tmp")
    staging.write_text(value)
    staging.replace(path)


if __name__ == "__main__":
    write_once(POSTGRES_PASSWORD, token_hex(24))
    write_once(S3_CONFIG, json.dumps({
        "identities": [{
            "name": "cinema",
            "credentials": [{"accessKey": token_hex(12), "secretKey": token_hex(20)}],
            "actions": ["Admin"],
        }]
    }))
