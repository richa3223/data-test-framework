import base64
import json
from typing import Union


def parse_synapse_script_arguments(encoded_args: str) -> Union[dict, list]:
    """Parse the base64 encoded json arguments passed to a synapse script

    Args:
        encoded_args (str): a urlsafe_b64 encoded string containing a json object

    Returns:
        dict: The dict representation of the arguments
    """
    return json.loads(
        base64.b64decode(encoded_args.encode("ascii")).decode("ascii")
    )
