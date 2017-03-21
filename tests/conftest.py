import pytest
import datapackage

@pytest.fixture
def towns_and_counties():
    towns = datapackage.DataPackage(
        'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json')
    counties = datapackage.DataPackage(
        'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-county-list/master/datapackage.json')
    town_name_list = [t['Town'] for t in towns.resources[0].data]
    county_name_list = [c['County'] for c in counties.resources[0].data]
    return town_name_list + county_name_list
