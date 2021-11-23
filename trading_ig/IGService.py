#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
IG Markets REST API Library for Python
https://labs.ig.com/rest-trading-api-reference
Original version by Lewis Barber - 2014 - https://uk.linkedin.com/in/lewisbarber/
Modified by Femto Trader - 2014-2015 - https://github.com/femtotrader/
"""  # noqa
import logging
import time
from trading_ig.utils import create_logger

from urllib.parse import urlparse, parse_qs
from datetime import timedelta, datetime
from trading_ig.utils import conv_datetime, conv_to_ms
from trading_ig.Exceptions import IGException
from trading_ig.SessionHandler import IGSessionHandler

logger = create_logger("rest", "log_rest.log")

class IGService:
    D_BASE_URL = {
        "live": "https://api.ig.com/gateway/deal",
        "demo": "https://demo-api.ig.com/gateway/deal",
    }

    _refresh_token = None
    _valid_until = None

    def __init__(self, config, acc_type="demo"):
        """Constructor, calls the method required to connect to
        the API (accepts acc_type = LIVE or DEMO)"""


        try:
            self.BASE_URL = self.D_BASE_URL[acc_type.lower()]
        except Exception:
            raise IGException("Invalid account type '%s', please provide LIVE or DEMO" % acc_type)

        self.crud_session = IGSessionHandler(self.BASE_URL, config)

    # --------- END -------- #

    # ------ DATAFRAME TOOLS -------- #

    @staticmethod
    def colname_unique(d_cols):
        """Returns a set of column names (unique)"""
        s = set()
        for _, lst in d_cols.items():
            for colname in lst:
                s.add(colname)
        return s

    @staticmethod
    def expand_columns(data, d_cols, flag_col_prefix=False, col_overlap_allowed=None):
        """Expand columns"""
        if col_overlap_allowed is None:
            col_overlap_allowed = []
        for (col_lev1, lst_col) in d_cols.items():
            ser = data[col_lev1]
            del data[col_lev1]
            for col in lst_col:
                if col not in data.columns or col in col_overlap_allowed:
                    if flag_col_prefix:
                        colname = col_lev1 + "_" + col
                    else:
                        colname = col
                    data[colname] = ser.map(lambda x: x[col], na_action='ignore')
                else:
                    raise (NotImplementedError("col overlap: %r" % col))
        return data

    # -------- END ------- #

    # -------- ACCOUNT ------- #

    def create_session(self, version):
        return self.crud_session.create_session(version=version)

    def fetch_accounts(self):
        """Returns a list of accounts belonging to the logged-in client"""
        version = "1"
        params = {}
        endpoint = "/accounts"
        return self.crud_session.read(endpoint, params,version)

    def fetch_account_preferences(self):
        """
        Gets the preferences for the logged in account
        :param session: session object. Optional
        :type session: requests.Session
        :return: preference values
        :rtype: dict
        """
        version = "1"
        params = {}
        endpoint = "/accounts/preferences"
        return self.crud_session.read(endpoint, params,version)

    def update_account_preferences(self, trailing_stops_enabled=False):
        """
        Updates the account preferences. Currently only one value supported - trailing stops
        :param trailing_stops_enabled: whether trailing stops should be enabled for the account
        :type trailing_stops_enabled: bool
        :param session: session object. Optional
        :type session: requests.Session
        :return: status of the update request
        :rtype: str
        """
        version = "1"
        params = {}
        endpoint = "/accounts/preferences"
        params['trailingStopsEnabled'] = 'true' if trailing_stops_enabled else 'false'
        data = self.crud_session.update(endpoint, params,version)
        return data['status']

    def fetch_account_activity_by_period(self, milliseconds):
        """
        Returns the account activity history for the last specified period
        """
        version = "1"
        milliseconds = conv_to_ms(milliseconds)
        params = {}
        url_params = {"milliseconds": milliseconds}
        endpoint = "/history/activity/{milliseconds}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_account_activity_by_date(self, from_date: datetime, to_date: datetime):
        """
        Returns the account activity history for period between the specified dates
        """
        version = "1"
        if from_date is None or to_date is None:
            raise IGException("Both from_date and to_date must be specified")
        if from_date > to_date:
            raise IGException("from_date must be before to_date")

        params = {}
        url_params = {
            "fromDate": from_date.strftime('%d-%m-%Y'),
            "toDate": to_date.strftime('%d-%m-%Y')
        }
        endpoint = "/history/activity/{fromDate}/{toDate}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_account_activity_v2(
            self,
            from_date: datetime = None,
            to_date: datetime = None,
            max_span_seconds: int = None,
            page_size: int = 20):

        """
        Returns the account activity history (v2)

        If the result set spans multiple 'pages', this method will automatically get all the results and
        bundle them into one object.

        :param from_date: start date and time. Optional
        :type from_date: datetime
        :param to_date: end date and time. A date without time refers to the end of that day. Defaults to
        today. Optional
        :type to_date: datetime
        :param max_span_seconds: Limits the timespan in seconds through to current time (not applicable if a
        date range has been specified). Default 600. Optional
        :type max_span_seconds: int
        :param page_size: number of records per page. Default 20. Optional. Use 0 to turn off paging
        :type page_size: int
        :param session: session object. Optional
        :type session: Session
        :return: results set
        :rtype: Pandas DataFrame if configured, otherwise a dict
        """

        version = "2"
        params = {}
        if from_date:
            params["from"] = from_date.strftime('%Y-%m-%dT%H:%M:%S')
        if to_date:
            params["to"] = to_date.strftime('%Y-%m-%dT%H:%M:%S')
        if max_span_seconds:
            params["maxSpanSeconds"] = max_span_seconds
        params["pageSize"] = page_size
        endpoint = "/history/activity/"
        data = {}
        activities = []
        pagenumber = 1
        more_results = True

        while more_results:
            params["pageNumber"] = pagenumber
            data = self.crud_session.read(endpoint, params,version)
            data = self.parse_response(response.text)
            activities.extend(data["activities"])
            page_data = data["metadata"]["pageData"]
            if page_data["totalPages"] == 0 or \
                    (page_data["pageNumber"] == page_data["totalPages"]):
                more_results = False
            else:
                pagenumber += 1

        data["activities"] = activities
        return data

    def fetch_account_activity(
            self,
            from_date: datetime = None,
            to_date: datetime = None,
            detailed=False,
            deal_id: str = None,
            fiql_filter: str = None,
            page_size: int = 50):

        """
        Returns the account activity history (v3)

        If the result set spans multiple 'pages', this method will automatically get all the results and
        bundle them into one object.

        :param from_date: start date and time. Optional
        :type from_date: datetime
        :param to_date: end date and time. A date without time refers to the end of that day. Defaults to
        today. Optional
        :type to_date: datetime
        :param detailed: Indicates whether to retrieve additional details about the activity. Default False. Optional
        :type detailed: bool
        :param deal_id: deal ID. Optional
        :type deal_id: str
        :param fiql_filter: FIQL filter (supported operators: ==|!=|,|;). Optional
        :type fiql_filter: str
        :param page_size: page size (min: 10, max: 500). Default 50. Optional
        :type page_size: int
        :param session: session object. Optional
        :type session: Session
        :return: results set
        :rtype: Pandas DataFrame if configured, otherwise a dict
        """

        version = "3"
        params = {}
        if from_date:
            params["from"] = from_date.strftime('%Y-%m-%dT%H:%M:%S')
        if to_date:
            params["to"] = to_date.strftime('%Y-%m-%dT%H:%M:%S')
        if detailed:
            params["detailed"] = "true"
        if deal_id:
            params["dealId"] = deal_id
        if fiql_filter:
            params["filter"] = fiql_filter

        params["pageSize"] = page_size
        endpoint = "/history/activity/"
        data = {}
        activities = []
        more_results = True

        while more_results:
            data = self.crud_session.read(endpoint, params,version)
            activities.extend(data["activities"])
            paging = data["metadata"]["paging"]
            if paging["next"] is None:
                more_results = False
            else:
                parse_result = urlparse(paging["next"])
                query = parse_qs(parse_result.query)
                logging.debug(f"fetch_account_activity() next query: '{query}'")
                if 'from' in query:
                    params["from"] = query["from"][0]
                else:
                    del params["from"]
                if 'to' in query:
                    params["to"] = query["to"][0]
                else:
                    del params["to"]

        data["activities"] = activities
        return data

    @staticmethod
    def format_activities(data):
        data = data.rename(columns={'details.marketName': 'marketName',
                                    'details.goodTillDate': 'goodTillDate',
                                    'details.currency': 'currency',
                                    'details.direction': 'direction',
                                    'details.level': 'level',
                                    'details.stopLevel': 'stopLevel',
                                    'details.stopDistance': 'stopDistance',
                                    'details.guaranteedStop': 'guaranteedStop',
                                    'details.trailingStopDistance': 'trailingStopDistance',
                                    'details.trailingStep': 'trailingStep',
                                    'details.limitLevel': 'limitLevel',
                                    'details.limitDistance': 'limitDistance'})

        cols = data.columns.tolist()
        cols = cols[2:] + cols[:2]
        data = data[cols]

        return data

    def fetch_transaction_history_by_type_and_period(
        self, milliseconds, trans_type
    ):
        """Returns the transaction history for the specified transaction
        type and period"""
        version = "1"
        milliseconds = conv_to_ms(milliseconds)
        params = {}
        url_params = {"milliseconds": milliseconds, "trans_type": trans_type}
        endpoint = "/history/transactions/{trans_type}/{milliseconds}".format(
            **url_params
        )
        return self.crud_session.read(endpoint, params,version)

    def fetch_transaction_history(
        self,
        trans_type=None,
        from_date=None,
        to_date=None,
        max_span_seconds=None,
        page_size=None,
        page_number=None
    ):
        """Returns the transaction history for the specified transaction
        type and period"""
        version = "2"
        params = {}
        if trans_type:
            params["type"] = trans_type
        if from_date:
            if hasattr(from_date, "isoformat"):
                from_date = from_date.isoformat()
            params["from"] = from_date
        if to_date:
            if hasattr(to_date, "isoformat"):
                to_date = to_date.isoformat()
            params["to"] = to_date
        if max_span_seconds:
            params["maxSpanSeconds"] = max_span_seconds
        if page_size:
            params["pageSize"] = page_size
        if page_number:
            params["pageNumber"] = page_number

        endpoint = "/history/transactions"

        return self.crud_session.read(endpoint, params,version)

    # -------- END -------- #

    # -------- DEALING -------- #

    def fetch_deal_by_deal_reference(self, deal_reference):
        """Returns a deal confirmation for the given deal reference"""
        version = "1"
        params = {}
        url_params = {"deal_reference": deal_reference}
        endpoint = "/confirms/{deal_reference}".format(**url_params)

        for i in range(5):
            data = self.crud_session.read(endpoint, params,version)
            if response.status_code == 404 or response.status_code == 405:
                logger.info("Deal reference %s not found, retrying." % deal_reference)
                time.sleep(1)
            else:
                break
        data = self.parse_response(response.text)
        return data

    def fetch_open_position_by_deal_id(self, deal_id):
        """Return the open position by deal id for the active account"""
        version = "2"
        params = {}
        url_params = {"deal_id": deal_id}
        endpoint = "/positions/{deal_id}".format(**url_params)
        for i in range(5):
            data = self.crud_session.read(endpoint, params,version)
            if response.status_code == 404:
                logger.info("Deal id %s not found, retrying." % deal_id)
                time.sleep(1)
            else:
                break
        data = self.parse_response(response.text)
        return data

    def fetch_open_positions(self, version='2'):
        """
        Returns all open positions for the active account. Supports both v1 and v2
        :param session: session object, otional
        :type session: Session
        :param version: API version, 1 or 2
        :type version: str
        :return: table of position data, one per row
        :rtype: pd.Dataframe
        """
        params = {}
        endpoint = "/positions"
        return self.crud_session.read(endpoint, params,version)

    def close_open_position(
        self,
        deal_id,
        direction,
        epic,
        expiry,
        level,
        order_type,
        quote_id,
        size
    ):
        """Closes one or more OTC positions"""
        version = "1"
        params = {
            "dealId": deal_id,
            "direction": direction,
            "epic": epic,
            "expiry": expiry,
            "level": level,
            "orderType": order_type,
            "quoteId": quote_id,
            "size": size,
        }
        endpoint = "/positions/otc"
        data = self.crud_session.delete(endpoint, params,version)
        deal_reference = data["dealReference"]
        return self.fetch_deal_by_deal_reference(deal_reference)

    def create_open_position(
        self,
        currency_code,
        direction,
        epic,
        expiry,
        force_open,
        guaranteed_stop,
        level,
        limit_distance,
        limit_level,
        order_type,
        quote_id,
        size,
        stop_distance,
        stop_level,
        trailing_stop,
        trailing_stop_increment
    ):
        """Creates an OTC position"""
        version = "2"
        params = {
            "currencyCode": currency_code,
            "direction": direction,
            "epic": epic,
            "expiry": expiry,
            "forceOpen": force_open,
            "guaranteedStop": guaranteed_stop,
            "level": level,
            "limitDistance": limit_distance,
            "limitLevel": limit_level,
            "orderType": order_type,
            "quoteId": quote_id,
            "size": size,
            "stopDistance": stop_distance,
            "stopLevel": stop_level,
            "trailingStop": trailing_stop,
            "trailingStopIncrement": trailing_stop_increment,
        }

        endpoint = "/positions/otc"

        data = self.crud_session.create(endpoint, params,version)

        deal_reference = data["dealReference"]
        return self.fetch_deal_by_deal_reference(deal_reference)

    def update_open_position(
            self,
            limit_level,
            stop_level,
            deal_id,
            guaranteed_stop=False,
            trailing_stop=False,
            trailing_stop_distance=None,
            trailing_stop_increment=None,
            version='2'):
        """Updates an OTC position"""
        params = {}
        if limit_level is not None:
            params["limitLevel"] = limit_level
        if stop_level is not None:
            params["stopLevel"] = stop_level
        if guaranteed_stop:
            params["guaranteedStop"] = 'true'
        if trailing_stop:
            params["trailingStop"] = 'true'
        if trailing_stop_distance is not None:
            params["trailingStopDistance"] = trailing_stop_distance
        if trailing_stop_increment is not None:
            params["trailingStopIncrement"] = trailing_stop_increment

        url_params = {"deal_id": deal_id}
        endpoint = "/positions/otc/{deal_id}".format(**url_params)
        data = self.crud_session.update(endpoint, params,version)

        deal_reference = data["dealReference"]
        return self.fetch_deal_by_deal_reference(deal_reference)

    def fetch_working_orders(self, version='2'):
        """Returns all open working orders for the active account"""
        params = {}
        endpoint = "/workingorders"
        return self.crud_session.read(endpoint, params,version)

    def create_working_order(
        self,
        currency_code,
        direction,
        epic,
        expiry,
        guaranteed_stop,
        level,
        size,
        time_in_force,
        order_type,
        limit_distance=None,
        limit_level=None,
        stop_distance=None,
        stop_level=None,
        good_till_date=None,
        deal_reference=None,
        force_open=False,
    ):
        """Creates an OTC working order"""
        version = "2"
        if good_till_date is not None and type(good_till_date) is not int:
            good_till_date = conv_datetime(good_till_date, version)

        params = {
            "currencyCode": currency_code,
            "direction": direction,
            "epic": epic,
            "expiry": expiry,
            "guaranteedStop": guaranteed_stop,
            "level": level,
            "size": size,
            "timeInForce": time_in_force,
            "type": order_type,
        }
        if limit_distance:
            params["limitDistance"] = limit_distance
        if limit_level:
            params["limitLevel"] = limit_level
        if stop_distance:
            params["stopDistance"] = stop_distance
        if stop_level:
            params["stopLevel"] = stop_level
        if deal_reference:
            params["dealReference"] = deal_reference
        if force_open:
            params["force_open"] = 'true'
        if good_till_date:
            params["goodTillDate"] = good_till_date

        endpoint = "/workingorders/otc"

        data = self.crud_session.create(endpoint, params,version)

        deal_reference = data["dealReference"]
        return self.fetch_deal_by_deal_reference(deal_reference)

    def delete_working_order(self, deal_id):
        """Deletes an OTC working order"""
        version = "2"
        params = {}
        url_params = {"deal_id": deal_id}
        endpoint = "/workingorders/otc/{deal_id}".format(**url_params)
        data = self.crud_session.delete(endpoint, params,version)

        deal_reference = data["dealReference"]
        return self.fetch_deal_by_deal_reference(deal_reference)

    def update_working_order(
        self,
        good_till_date,
        level,
        limit_distance,
        limit_level,
        stop_distance,
        stop_level,
        guaranteed_stop,
        time_in_force,
        order_type,
        deal_id,
    ):
        """Updates an OTC working order"""
        version = "2"
        if good_till_date is not None and type(good_till_date) is not int:
            good_till_date = conv_datetime(good_till_date, version)
        params = {
            "goodTillDate": good_till_date,
            "limitDistance": limit_distance,
            "level": level,
            "limitLevel": limit_level,
            "stopDistance": stop_distance,
            "stopLevel": stop_level,
            "guaranteedStop": guaranteed_stop,
            "timeInForce": time_in_force,
            "type": order_type,
        }
        url_params = {"deal_id": deal_id}
        endpoint = "/workingorders/otc/{deal_id}".format(**url_params)
        data = self.crud_session.update(endpoint, params,version)

        deal_reference = data["dealReference"]
        return self.fetch_deal_by_deal_reference(deal_reference)

    # -------- END -------- #

    # -------- MARKETS -------- #

    def fetch_client_sentiment_by_instrument(self, market_id):
        """Returns the client sentiment for the given instrument's market"""
        version = "1"
        params = {}
        if isinstance(market_id, (list,)):
            market_ids = ",".join(market_id)
            url_params = {"market_ids": market_ids}
            endpoint = "/clientsentiment/?marketIds={market_ids}".format(**url_params)
        else:
            url_params = {"market_id": market_id}
            endpoint = "/clientsentiment/{market_id}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_related_client_sentiment_by_instrument(self, market_id):
        """Returns a list of related (also traded) client sentiment for
        the given instrument's market"""
        version = "1"
        params = {}
        url_params = {"market_id": market_id}
        endpoint = "/clientsentiment/related/{market_id}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_top_level_navigation_nodes(self):
        """Returns all top-level nodes (market categories) in the market
        navigation hierarchy."""
        version = "1"
        params = {}
        endpoint = "/marketnavigation"
        return self.crud_session.read(endpoint, params,version)

    def fetch_sub_nodes_by_node(self, node):
        """Returns all sub-nodes of the given node in the market
        navigation hierarchy"""
        version = "1"
        params = {}
        url_params = {"node": node}
        endpoint = "/marketnavigation/{node}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_market_by_epic(self, epic):
        """Returns the details of the given market"""
        version = "3"
        params = {}
        url_params = {"epic": epic}
        endpoint = "/markets/{epic}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_markets_by_epics(self, epics, detailed=True, version='2'):
        """
        Returns the details of the given markets
        :param epics: comma separated list of epics
        :type epics: str
        :param detailed: Whether to return detailed info or snapshot data only. Only supported for
        version 2. Optional, default True
        :type detailed: bool
        :param session: session object. Optional, default None
        :type session: requests.Session
        :param version: IG API method version. Optional, default '2'
        :type version: str
        :return: list of market details
        """
        params = {"epics": epics}
        if version == '2':
            params["filter"] = 'ALL' if detailed else 'SNAPSHOT_ONLY'
        endpoint = "/markets"
        return self.crud_session.read(endpoint, params,version)['marketDetails']

    def search_markets(self, search_term):
        """Returns all markets matching the search term"""
        version = "1"
        endpoint = "/markets"
        params = {"searchTerm": search_term}
        return self.crud_session.read(endpoint, params,version)

    def fetch_historical_prices_by_epic(
        self,
        epic,
        start_date=None,
        end_date=None,
        numpoints=None,
        pagesize=20,
        format=None,
        wait=1
    ):

        """
        Fetches historical prices for the given epic.

        This method wraps the IG v3 /prices/{epic} endpoint. With this method you can
        choose to get either a fixed number of prices in the past, or to get the
        prices between two points in time. By default it will return the last 10
        prices at 1 minute resolution.

        If the result set spans multiple 'pages', this method will automatically
        get all the results and bundle them into one object.

        :param epic: (str) The epic key for which historical prices are being
            requested
        :param resolution: (str, optional) timescale resolution. Expected values
            are 1Min, 2Min, 3Min, 5Min, 10Min, 15Min, 30Min, 1H, 2H, 3H, 4H, D,
            W, M. Default is 1Min
        :param start_date: (datetime, optional) date range start, format
            yyyy-MM-dd'T'HH:mm:ss
        :param end_date: (datetime, optional) date range end, format
            yyyy-MM-dd'T'HH:mm:ss
        :param numpoints: (int, optional) number of data points. Default is 10
        :param pagesize: (int, optional) number of data points. Default is 20
        :param session: (Session, optional) session object
        :param format: (function, optional) function to convert the raw
            JSON response
        :param wait: (int, optional) how many seconds to wait between successive
            calls in a multi-page scenario. Default is 1
        :returns: Pandas DataFrame if configured, otherwise a dict
        :raises Exception: raises an exception if any error is encountered
        """

        version = "3"
        params = {}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date
        if numpoints:
            params["max"] = numpoints
        params["pageSize"] = pagesize
        url_params = {"epic": epic}
        endpoint = "/prices/{epic}".format(**url_params)
        prices = []
        pagenumber = 1
        more_results = True

        while more_results:
            params["pageNumber"] = pagenumber
            data = self.crud_session.read(endpoint, params,version)
            prices.extend(data["prices"])
            page_data = data["metadata"]["pageData"]
            if page_data["totalPages"] == 0 or \
                    (page_data["pageNumber"] == page_data["totalPages"]):
                more_results = False
            else:
                pagenumber += 1
            time.sleep(wait)

        data["prices"] = prices

        self.log_allowance(data["metadata"])
        return data

    def fetch_historical_prices_by_epic_and_num_points(self, epic, resolution,numpoints,format=None):
        """Returns a list of historical prices for the given epic, resolution,
        number of points"""
        version = "2"
        resolution = resolution
        params = {}
        url_params = {"epic": epic, "resolution": resolution, "numpoints": numpoints}
        endpoint = "/prices/{epic}/{resolution}/{numpoints}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def fetch_historical_prices_by_epic_and_date_range(self, epic, resolution, start_date, end_date, format=None, version='2'):
        """
        Returns a list of historical prices for the given epic, resolution, multiplier and date range. Supports
        both versions 1 and 2
        :param epic: IG epic
        :type epic: str
        :param resolution: timescale for returned data. Expected values 'M', 'D', '1H' etc
        :type resolution: str
        :param start_date: start date for returned data. For v1, format '2020:09:01-00:00:00', for v2 use
            '2020-09-01 00:00:00'
        :type start_date: str
        :param end_date: end date for returned data. For v1, format '2020:09:01-00:00:00', for v2 use
            '2020-09-01 00:00:00'
        :type end_date: str
        :param session: HTTP session
        :type session: requests.Session
        :param format: function defining how the historic price data should be converted into a Dataframe
        :type format: function
        :param version: API method version
        :type version: str
        :return: historic data
        :rtype: dict, with 'prices' element as pandas.Dataframe
        """
        resolution = resolution
        params = {}
        if version == '1':
            start_date = conv_datetime(start_date, version)
            end_date = conv_datetime(end_date, version)
            params = {"startdate": start_date, "enddate": end_date}
            url_params = {"epic": epic, "resolution": resolution}
            endpoint = "/prices/{epic}/{resolution}".format(**url_params)
        else:
            url_params = {"epic": epic, "resolution": resolution, "startDate": start_date, "endDate": end_date}
            endpoint = "/prices/{epic}/{resolution}/{startDate}/{endDate}".format(**url_params)
        data = self.crud_session.read(endpoint, params,version)
        del self.session.headers["VERSION"]
        return data

    def log_allowance(self, data):
        remaining_allowance = data['allowance']['remainingAllowance']
        allowance_expiry_secs = data['allowance']['allowanceExpiry']
        allowance_expiry = datetime.today() + timedelta(seconds=allowance_expiry_secs)
        logger.info("Historic price data allowance: %s remaining until %s" %
                    (remaining_allowance, allowance_expiry))

    # -------- END -------- #

    # -------- WATCHLISTS -------- #

    def fetch_all_watchlists(self):
        """Returns all watchlists belonging to the active account"""
        version = "1"
        params = {}
        endpoint = "/watchlists"
        return self.crud_session.read(endpoint, params,version)

    def create_watchlist(self, name, epics):
        """Creates a watchlist"""
        version = "1"
        params = {"name": name, "epics": epics}
        endpoint = "/watchlists"
        return self.crud_session.create(endpoint, params,version)

    def delete_watchlist(self, watchlist_id):
        """Deletes a watchlist"""
        version = "1"
        params = {}
        url_params = {"watchlist_id": watchlist_id}
        endpoint = "/watchlists/{watchlist_id}".format(**url_params)
        return self.crud_session.delete(endpoint, params,version)

    def fetch_watchlist_markets(self, watchlist_id):
        """Returns the given watchlist's markets"""
        version = "1"
        params = {}
        url_params = {"watchlist_id": watchlist_id}
        endpoint = "/watchlists/{watchlist_id}".format(**url_params)
        return self.crud_session.read(endpoint, params,version)

    def add_market_to_watchlist(self, watchlist_id, epic):
        """Adds a market to a watchlist"""
        version = "1"
        params = {"epic": epic}
        url_params = {"watchlist_id": watchlist_id}
        endpoint = "/watchlists/{watchlist_id}".format(**url_params)
        return self.crud_session.update(endpoint, params,version)

    def remove_market_from_watchlist(self, watchlist_id, epic):
        """Remove a market from a watchlist"""
        version = "1"
        params = {}
        url_params = {"watchlist_id": watchlist_id, "epic": epic}
        endpoint = "/watchlists/{watchlist_id}/{epic}".format(**url_params)
        return self.crud_session.delete(endpoint, params,version)

    # -------- END -------- #

    # -------- LOGIN -------- #

    def logout(self):
        """Log out of the current session"""
        version = "1"
        params = {}
        endpoint = "/session"
        self.crud_session.delete(endpoint, params,version)
        self.session.close()

    def switch_account(self, account_id, default_account):
        """Switches active accounts, optionally setting the default account"""
        version = "1"
        params = {"accountId": account_id, "defaultAccount": default_account}
        endpoint = "/session"
        return self.crud_session.update(endpoint, params,version)

    def read_session(self, fetch_session_tokens='false'):
        """Retrieves current session details"""
        version = "1"
        params = {"fetchSessionTokens": fetch_session_tokens}
        endpoint = "/session"
        return self.crud_session.read(endpoint, params,version)

    # -------- END -------- #

    # -------- GENERAL -------- #

    def get_client_apps(self):
        """Returns a list of client-owned applications"""
        version = "1"
        params = {}
        endpoint = "/operations/application"
        return self.crud_session.read(endpoint, params,version)

    def update_client_app(
        self,
        allowance_account_overall,
        allowance_account_trading,
        api_key,
        status
    ):
        """Updates an application"""
        version = "1"
        params = {
            "allowanceAccountOverall": allowance_account_overall,
            "allowanceAccountTrading": allowance_account_trading,
            "apiKey": api_key,
            "status": status,
        }
        endpoint = "/operations/application"
        return self.crud_session.update(endpoint, params,version)

    def disable_client_app_key(self):
        """
        Disables the current application key from processing further requests.
        Disabled keys may be re-enabled via the My Account section on
        the IG Web Dealing Platform.
        """
        version = "1"
        params = {}
        endpoint = "/operations/application/disable"
        return self.crud_session.update(endpoint, params,version)
