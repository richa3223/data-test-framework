from datetime import datetime, timedelta
import functools
from azure.common.credentials import get_cli_profile
from azure.cli.core._profile import _USER_ENTITY, _USER_TYPE, _SERVICE_PRINCIPAL


def is_running_as_sp(subscription_id):
    profile = get_cli_profile()
    account = profile.get_subscription(subscription_id)
    user_type = account[_USER_ENTITY].get(_USER_TYPE)
    return user_type == _SERVICE_PRINCIPAL


def get_current_user_without_domain():
    profile = get_cli_profile()
    return profile.get_current_account_user().split("@", 1)[0]


def timed_cache(**timedelta_kwargs):
    def _wrapper(f):
        update_delta = timedelta(**timedelta_kwargs)
        next_update = datetime.utcnow() + update_delta
        # Apply @lru_cache to f with no cache size limit
        f = functools.lru_cache(None)(f)

        @functools.wraps(f)
        def _wrapped(*args, **kwargs):
            nonlocal next_update
            now = datetime.utcnow()
            if now >= next_update:
                f.cache_clear()
                next_update = now + update_delta
            return f(*args, **kwargs)

        return _wrapped

    return _wrapper