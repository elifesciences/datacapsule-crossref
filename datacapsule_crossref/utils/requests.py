from __future__ import absolute_import

import requests
from requests.packages.urllib3 import Retry # pylint: disable=E0401


def configure_session_retry(
  session=None, max_retries=3, backoff_factor=1, status_forcelist=None,
  **kwargs):

  retry = Retry(
    connect=max_retries,
    read=max_retries,
    status_forcelist=status_forcelist,
    redirect=5,
    backoff_factor=backoff_factor
  )
  session.mount('http://', requests.adapters.HTTPAdapter(max_retries=retry, **kwargs))
  session.mount('https://', requests.adapters.HTTPAdapter(max_retries=retry, **kwargs))
