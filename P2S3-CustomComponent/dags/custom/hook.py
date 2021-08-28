from airflow.hooks.base_hook import BaseHook
import requests

class MovielensHook(BaseHook):
    
    DEFAULT_SCHEMA = "http"
    DEFAULT_PORT = 5000

    def __init__(self,conn_id):
        super().__init__()
        self._conn_id = conn_id

        #Cache connection in order not to request to Airflow metastore many times
        self._session = None
        self._base_url = None

    def get_conn(self):
        """
        Override get_conn method to setup connection to extenal system

        Returns the connection used by the hook for querying data.
        Should in principle not be used directly.
        """

        if self._session is None:
            # Fetch config for the given connection (host, login, etc).
            config = self.get_connection(self._conn_id)

            if not config.host:
                raise ValueError(f"No host specified in connection {self._conn_id}")

            schema = config.schema or self.DEFAULT_SCHEMA
            port = config.port or self.DEFAULT_PORT

            self._base_url = f"{schema}://{config.host}:{port}"

            # Build our session instance, which we will use for any
            # requests to the API.
            self._session = requests.Session()

            if config.login:
                self._session.auth = (config.login, config.password)

        return self._session, self._base_url

    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None

     # API methods:

    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        """
        Fetches ratings between the given start/end date.
        Parameters
        ----------
        start_date : str
            Start date to start fetching ratings from (inclusive). Expected
            format is YYYY-MM-DD (equal to Airflow's ds formats).
        end_date : str
            End date to fetching ratings up to (exclusive). Expected
            format is YYYY-MM-DD (equal to Airflow's ds formats).
        batch_size : int
            Size of the batches (pages) to fetch from the API. Larger values
            mean less requests, but more data transferred per request.
        """

        yield from self._get_with_pagination(
            endpoint="/ratings",
            params={"start_date": start_date, "end_date": end_date},
            batch_size=batch_size,
        )

    def _get_with_pagination(self, endpoint, params, batch_size=100):
        """
        Fetches records using a get request with given url/params,
        taking pagination into account.
        """

        session, base_url = self.get_conn()
        url = base_url + endpoint

        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(
                url, params={**params, **{"offset": offset, "limit": batch_size}}
            )
            response.raise_for_status()
            response_json = response.json()

            yield from response_json["result"]

            offset += batch_size
            total = response_json["total"]