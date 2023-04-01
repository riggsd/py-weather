#!/usr/bin/env python3
"""
A simple client for IBM / The Weather Company's Personal Weather Station (PWS) API

See: https://docs.google.com/document/d/1eKCnKXI9xnoMGRRzOL1xPCBihNV2rOet08qpE_gArAY/view
"""

import datetime
import os
from typing import Iterator

import requests


__all__ = "WeatherAPI", "Units"


class Units:
    """The unit of measure for the response."""
    IMPERIAL = "e"
    ENGLISH  = "e"
    METRIC   = "m"
    HYBRID   = "h"
    UK       = "h"


class WeatherAPI:
    """
    A simple client for IBM / The Weather Company's Personal Weather Station (PWS) API

    See: https://docs.google.com/document/d/1eKCnKXI9xnoMGRRzOL1xPCBihNV2rOet08qpE_gArAY/view
    """

    API_ROOT = "https://api.weather.com/v2"

    def __init__(self, api_key: str = None, station: str = None, units: str = Units.IMPERIAL):
        """
        A WeatherAPI client instance. API Key is required and must be issued by The Weather Company. You may specify a
        PWS station ID and it will be used by default for all service calls; or you may specify a station ID with each
        individual service call.

        :param str api_key: Your issued API Key from The Weather Company; can be specified as an arg or the
                            `WX_API_KEY` environment variable
        :param str station: default PWS station ID; can be specified as an arg or the `WX_STATION` environment variable
        :param str units: Units of measure for response values; see `Units` enum for choices
        """
        self._params = {
            "apiKey": api_key or os.environ.get("WX_API_KEY"),
            "stationId": station or os.environ.get("WX_STATION"),
            "units": units,
            "format": "json",
            "numericPrecision": "decimal",
        }

    def __repr__(self):
        return "WeatherAPI('{}', '{}')".format(self._params["apiKey"], self._params["stationId"])

    def _transform(self, record: dict) -> dict:
        """Custom transform for friendlier Python response objects"""
        for attr in "imperial", "metric", "uk_hybrid":
            if attr in record:
                record.update(record[attr])
                del record[attr]
                break
        # record["obsTimeUtc"] = datetime.datetime.fromisoformat(record["obsTimeUtc"])
        # record["obsTimeLocal"] = datetime.datetime.fromisoformat(record["obsTimeLocal"])
        return record

    def current(self, station: str = None) -> dict:
        """
        Personal Weather Stations (PWS) Current Conditions returns the current conditions observations for the current
        record.

        See: https://docs.google.com/document/d/1KGb8bTVYRsNgljnNH67AMhckY8AQT2FVwZ9urj8SWBs/view
        """
        params = self._params | {"stationId": station} if station else self._params
        response = requests.get(self.API_ROOT + "/pws/observations/current", params=params)
        response.raise_for_status()
        observation = response.json()["observations"][0]
        return self._transform(observation)

    def dailysummary(self, station: str = None) -> list[dict]:
        """
        Personal Weather Station (PWS) Daily Summary Historical Observations returns the daily summary of daily
        observations for each day's observations report.

        See: https://docs.google.com/document/d/1OlAIqLb8kSfNV_Uz1_3je2CGqSnynV24qGHHrLWn7O8/view
        """
        params = self._params | {"stationId": station} if station else self._params
        response = requests.get(self.API_ROOT + "/pws/dailysummary/7day", params=params)
        response.raise_for_status()
        summaries = response.json()["summaries"]
        return [self._transform(s) for s in summaries]

    def observations_1day_highres(self, station: str = None) -> list[dict]:
        """
        Personal Weather Station (PWS) Rapid Historical Observations returns the daily observations records in rapid
        frequency as frequent as every 5 minutes. Actual frequency of reports ranges and is dependent on how frequently
        an individual Personal Weather Station (PWS) reports data.

        See: https://docs.google.com/document/d/1wzejRIUONpdGv0P3WypGEqvSmtD5RAsNOOucvdNRi6k/view
        """
        params = self._params | {"stationId": station} if station else self._params
        response = requests.get(self.API_ROOT + "/pws/observations/all/1day", params=params)
        response.raise_for_status()
        observations = response.json()["observations"]
        return [self._transform(o) for o in observations]

    def observations_7day_hourly(self, station: str = None) -> list[dict]:
        """
        Personal Weather Stations (PWS) Hourly Historical Observations returns the hourly records for each day's
        observations report.

        See: https://docs.google.com/document/d/1GsvGH7TEog_z63ZawX0lHohISBv4qb0aIh8WoHHINF0/view
        """
        params = self._params | {"stationId": station} if station else self._params
        response = requests.get(self.API_ROOT + "/pws/observations/hourly/7day", params=params)
        response.raise_for_status()
        observations = response.json()["observations"]
        return [self._transform(o) for o in observations]

    def history_daily(self, date: datetime.date, station: str = None) -> dict:
        """
        Personal Weather Stations (PWS) Historical Data returns the historical PWS data for a single date, returning
        summary data for the entire day

        See: https://docs.google.com/document/d/1w8jbqfAk0tfZS5P7hYnar1JiitM0gQZB-clxDfG3aD0/view
        """
        if isinstance(date, str):
            date = datetime.datetime.strptime(date.replace("-", ""), "%Y%m%d")
        params = self._params | {"stationId": station} if station else self._params
        params["date"] = date.strftime("%Y%m%d")
        response = requests.get(self.API_ROOT + "/pws/history/daily", params=params)
        response.raise_for_status()
        observations = response.json()["observations"]
        if not observations:
            return None
        return self._transform(observations[0])

    def history_daily_range(self, start: datetime.date = None, end: datetime.date = None, station: str = None) -> Iterator[dict]:
        """Generate daily history over a range of days, most recent first"""
        if not start:
            start = datetime.date.today()
        elif start < end:
            start, end = end, start
        current = start
        while end is None or current >= end:
            result = self.history_daily(current, station)
            if not result:
                return
            yield result
            current -= datetime.timedelta(days=1)

    def history_hourly(self, date: datetime.date, station: str = None) -> list[dict]:
        """
        Personal Weather Stations (PWS) Historical Data returns the historical PWS data for a single date, returning
        hourly data

        See: https://docs.google.com/document/d/1w8jbqfAk0tfZS5P7hYnar1JiitM0gQZB-clxDfG3aD0/view
        """
        if isinstance(date, str):
            date = datetime.datetime.strptime(date.replace("-", ""), "%Y%m%d")
        params = self._params | {"stationId": station} if station else self._params
        params["date"] = date.strftime("%Y%m%d")
        response = requests.get(self.API_ROOT + "/pws/history/hourly", params=params)
        response.raise_for_status()
        observations = response.json()["observations"]
        return [self._transform(o) for o in observations]

    def history_hourly_range(self, start: datetime.date = None, end: datetime.date = None, station: str = None) -> Iterator[dict]:
        """Generate hourly history per day over a range of days, most recent first"""
        if not start:
            start = datetime.date.today()
        elif start < end:
            start, end = end, start
        current = start
        while end is None or current >= end:
            results = self.history_hourly(current, station)
            if not results:
                return
            yield from reversed(results)
            current -= datetime.timedelta(days=1)


if __name__ == '__main__':
    import json

    client = WeatherAPI()
    # print(json.dumps(client.current(), indent='\t'))
    # print(json.dumps(client.dailysummary(), indent='\t'))
    # print(json.dumps(client.observations_1day_highres(), indent='\t'))
    print(json.dumps(client.observations_7day_hourly(), indent='\t'))
    # print(json.dumps(client.history_daily(datetime.date.today()), indent='\t'))
    # print(json.dumps(client.history_hourly(datetime.date.today()), indent='\t'))
