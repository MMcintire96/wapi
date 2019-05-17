import requests
import json
from xml.etree import ElementTree as ET

class DataAPI():
    def __init__(self):
        self.base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
        self.headers = {"token": "IPPiOrvnezfbLSjlGpnzgydQgupruTkM"}

    def get(self, endpoints):
        """Returns response in json or throws ValueError"""
        r = requests.get(f"{self.base_url}{endpoints}",
                headers=self.headers)
        if 'xml' in r.headers['Content-Type']:
            tree = ET.fromstring(r.text)
            error_string = ""
            for elem in tree[1:]:
                error_string += elem.text + " "
            raise ValueError(error_string)
        return r.json()


    def list_info(self, endpoint):
        """Returns a list of information about an endpoint
        enpoints:
            datasets
            datacategories
            datatypes
            locationcategories
            locations
            stations
        """
        return self.get(endpoint)


    def yield_data(self, **kwargs):
        """Returns the data for the datasetid
	View all expected params at:
	    https://www.ncdc.noaa.gov/cdo-web/webservices/v2#data
        Kwargs examples and values:
            datasetid = "GHCND" Required
            datatypeid = "ACMH" Optional
            locationid = "FIPS:37" Optional
            stationid = "GHCND:US1NCBC0005" Optional
            startdate = "2010-01-01" Required
            enddate = "2010-02-01" Required
            units = "metric" Optional
            sortfield = "name" Optional
            sortorder = "desc" Optional
            limit = "42" Optional
            offset = "24" Optional
            includemetadata = "false" Optional
        """
        endpoints = "/data?"
        for key,val in kwargs.items():
            endpoints += key + "=" + val + "&"
        meta = kwargs.get('includemetadata')
        r = self.get(endpoints[:-1])
        if meta:
            if meta == 'false':
                for item in r['results']:
                    yield item
            elif meta == 'true':
                data = [r['metadata']]
                for item in self.get(endpoints[:-1])['results']:
                    data.append(item)
                for d in data:
                    yield d
        for item in r['results']:
            yield item


if __name__ == "__main__":
    d = DataAPI()
    print(d.list_info('/datasets'))
    data = d.yield_data(datasetid='GSOM', datatypeid='PRCP', locationid="FIPS:37",
            startdate="2010-05-01", enddate="2010-05-31", units="metric")
    for d in data:
        print(json.dumps(d, indent=2))
