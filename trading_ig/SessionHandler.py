from requests import Session
from retry import retry
import json
# from datetime import datetime
import datetime
from trading_ig.Exceptions import IGException, ApiExceededException, IGExceptionSessionReset
from trading_ig.utils import create_logger

logger = create_logger("session_handler", "log_session_handler.log")

class IGSessionHandler:
    """Session with CRUD operation"""

    def __init__(self, base_url, config):
        self.BASE_URL = base_url
        self.API_KEY = config.api_key
        self.IG_USERNAME = config.username
        self.IG_PASSWORD = config.password
        self.ACC_NUMBER = config.acc_number

        self._refresh_token = None
        self._valid_until = None

        self.session = Session()

        self.session.headers.update({
            "X-IG-API-KEY": self.API_KEY,
            'Content-Type': 'application/json',
            'Accept': 'application/json; charset=UTF-8'
        })

    def _handle_oauth(self, oauth):
        """
        Handle the v3 headers during session creation and refresh
        :param oauth: 'oauth' portion of the response body
        :type oauth: dict
        """
        access_token = oauth['access_token']
        token_type = oauth['token_type']
        self.session.headers.update({'Authorization': f"{token_type} {access_token}"})
        self._refresh_token = oauth['refresh_token']
        validity = int(oauth['expires_in'])
        self._valid_until = datetime.datetime.now() + datetime.timedelta(seconds=validity)


    def refresh_session(self, version='1'):
        """
        Refreshes a v3 session. Tokens only last for 60 seconds, so need to be renewed regularly
        :param session: HTTP session object
        :type session: requests.Session
        :param version: API method version
        :type version: str
        :return: HTTP status code
        :rtype: int
        """
        logger.info(f"Refreshing session '{self.IG_USERNAME}'")
        params = {"refresh_token": self._refresh_token}
        endpoint = "/session/refresh-token"
        response = self.create(endpoint, params,version)
        self._handle_oauth(json.loads(response.text))
        return response.status_code

    def handle_session_tokens(self, response):
        """
        Copy session tokens from response to headers, so they will be present for all future requests
        :param response: HTTP response object
        :type response: requests.Response
        :param session: HTTP session object
        :type session: requests.Session
        """
        if "CST" in response.headers:
            self.session.headers['CST'] = response.headers['CST']
        if "X-SECURITY-TOKEN" in response.headers:
            self.session.headers['X-SECURITY-TOKEN'] = response.headers['X-SECURITY-TOKEN']

    def _manage_headers(self, response):
        """
        Manages authentication headers - different behaviour depending on the session creation version
        :param response: HTTP response
        :type response: requests.Response
        """
        # handle v1 and v2 logins
        self.handle_session_tokens(response)
        # handle v3 logins
        if response.text:
            self.session.headers.update({'IG-ACCOUNT-ID': self.ACC_NUMBER})
            payload = json.loads(response.text)
            if 'oauthToken' in payload:
                self._handle_oauth(payload['oauthToken'])

    
    def _api_limit_hit(self, response_text):
        # note we don't check for historical data allowance - it only gets reset once a week
        return 'exceeded-api-key-allowance' in response_text or \
               'exceeded-account-allowance' in response_text or \
               'exceeded-account-trading-allowance' in response_text

    def _handle_response(self, response):
        """Creates a CRUD request and returns response"""
        if response.status_code >= 500:
            raise (IGException(f"Server problem: status code: {response.status_code}, reason: {response.reason}"))

        response.encoding = 'utf-8'
        if self._api_limit_hit(response.text):
            logger.debug("_handle_response > allowance exceeded")
            self._reset_session()
            raise ApiExceededException()
        
        response_json = json.loads(response.text)
        if "errorCode" in response_json:
            if "error.security.client-token-missing" in response_json["errorCode"]:
                logger.debug("_handle_response > token is missing")
                self._reset_session()
                raise IGExceptionSessionReset()
            else:
                logger.debug("_handle_response > other error")
                raise Exception(response_json["errorCode"])
        return response_json
        
    def _check_session(self):
        """
        Check the v3 session status before making an API request:
            - v3 tokens only last for 60 seconds
            - if possible, the session can be renewed with a special refresh token
            - if not, a new session will be created
        """
        logger.debug("Checking session status...")
        if self._valid_until is None or datetime.now() > self._valid_until:
            return
            
        if self._refresh_token:
            # we are in a v3,need to refresh
            try:
                logger.info("Current session has expired, refreshing...")
                self.refresh_session()
            except IGException:
                logger.info("Refresh failed, resetting session")
                self._reset_session()
    
    def _reset_session(self):
        logger.info("Nuking session, full reset.")
        self._refresh_token = None
        self._valid_until = None
        self.session.headers.pop('Authorization', None)
        self.create_session(version='3')

    def _url(self, endpoint):
        """Returns url from endpoint and base url"""
        return self.BASE_URL + endpoint

    def create_session(self, version='2'):
        """
        Creates a,obtaining tokens for subsequent API access

        ** April 2021 v3 has been implemented, but is not the default for now

        :param session: HTTP session
        :type session: requests.Session
        :param encryption: whether or not the password should be encrypted. Required for some regions
        :type encryption: Boolean
        :param version: API method version
        :type version: str
        :return: JSON response body, parsed into dict
        :rtype: dict
        """
        if version == '3' and self.ACC_NUMBER is None:
            raise IGException('Account number must be set for v3 sessions')

        logger.info(f"Creating new v{version} session for user '{self.IG_USERNAME}' at '{self.BASE_URL}'")
        params = {"identifier": self.IG_USERNAME, "password": self.IG_PASSWORD}
        url = self._url("/session")
        response = self.session.post(url, data=json.dumps(params))
        self._manage_headers(response)
        return response
    
    def create(self, endpoint, params, version):
        """Create = POST"""
        self._check_session()
        url = self._url(endpoint)
        
        self.session.headers.update({'VERSION': version})
        response = self.session.post(url, data=json.dumps(params))
        logger.info(f"POST '{endpoint}', resp {response.status_code}")
        return self._handle_response(response)

    @retry((ApiExceededException, IGExceptionSessionReset), delay=2, tries=5, backoff=2, logger=logger)
    def read(self, endpoint, params, version):
        """Read = GET"""
        self._check_session()
        url = self._url(endpoint)
        
        self.session.headers.update({'VERSION': version})
        response = self.session.get(url, params=params)
        # handle 'read_session' with 'fetchSessionTokens=true'
        self.handle_session_tokens(response)
        logger.info(f"GET '{endpoint}', resp {response.status_code}")
        return self._handle_response(response)

    def update(self, endpoint, params,version):
        """Update = PUT"""
        self._check_session()
        url = self._url(endpoint)
        
        self.session.headers.update({'VERSION': version})
        response = self.session.put(url, data=json.dumps(params))
        logger.info(f"PUT '{endpoint}', resp {response.status_code}")
        return self._handle_response(response)

    def delete(self, endpoint, params,version):
        """Delete = POST"""
        self._check_session()
        url = self._url(endpoint)
        
        self.session.headers.update({'VERSION': version})
        self.session.headers.update({'_method': 'DELETE'})
        response = self.session.post(url, data=json.dumps(params))
        logger.info(f"DELETE (POST) '{endpoint}', resp {response.status_code}")
        del self.session.headers['_method']

        return self._handle_response(response)
