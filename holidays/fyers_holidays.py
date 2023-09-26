import os
import json
import requests
from datetime import datetime
from functools import lru_cache
from typing import (
    Literal, 
    NewType, 
    List, 
    Dict
)


DateFormat = NewType(name="dd-mm-YYYY", tp=str)

def fetch_holidays() -> List[Dict]:
    holiday_url = "https://fyers.in/holiday-data.json"

    try:
        res = requests.get(
            holiday_url
            )
        res.raise_for_status()
        if res.status_code == 200:
            return res.json()
    except (requests.exceptions.RequestException, Exception) as e:
        print(f"Error Occured :: {e}")

def manage_holiday_data(
        filename: str, 
        data: List[Dict] = None, 
        operation: Literal["r", "w"]="w"
        ) -> List[Dict]:
    
    with open(filename, operation) as json_file:
        if operation == "w":
            json.dump(
                data, 
                json_file,
                indent= 4
            )
        else:
            data = json.load(json_file)
            return data

def format_holidays_data(data: List[Dict]) -> List[Dict]:
    segments = set(segment['segment_name'] for i in data for segment in i['segments_closed'])

    data = [
        {
            'holiday_date': datetime.strptime(i['holiday_date'], '%B %d, %Y').strftime('%d-%m-%Y'),
            'holiday_day': i['holiday_day'],
            'holiday_name': i['holiday_name'],
            **{segment_name.replace(' & ', '&'): any(segment['segment_name'] == segment_name for segment in i['segments_closed']) for segment_name in segments}
        }
        for i in data
    ]

    return data


@lru_cache(maxsize= None)
def get_holidays(retry= 1) -> List[Dict]:
    holiday_data = os.path.join(os.path.dirname(__file__), 'holidays.json') 
    current_year = str(datetime.now().year)

    if os.path.exists(holiday_data):
        try:
            holidays = manage_holiday_data(
                                filename=holiday_data, 
                                operation="r"
                                )
            if holidays:
                if holidays[0]["holiday_date"].split("-")[2] == current_year:
                    return holidays
                else:
                    print("Old data. Retrying..")
                    get_holidays.cache_clear()
                    os.remove(holiday_data)
                    if retry <= 3:
                        return get_holidays(retry = retry+1)
            else:
                print("Empty file..")
                get_holidays.cache_clear()
                os.remove(holiday_data)
                if retry <= 3:
                    return get_holidays(retry = retry+1)
        except (json.decoder.JSONDecodeError, Exception) as e:
            print(f"Error :: {e} :: Retrying..")
            holidays = fetch_holidays()
            if holidays:
                holidays_list = format_holidays_data(holidays)
                manage_holiday_data(
                    filename= holiday_data,
                    data= holidays_list
                )
                return holidays_list
            else:
                get_holidays.cache_clear()
                if retry <= 3:
                    return get_holidays(retry = retry+1)
    else:
        holidays = fetch_holidays()
        if holidays:
            holidays_list = format_holidays_data(holidays)
            manage_holiday_data(
                    filename= holiday_data,
                    data= holidays_list
                )
            return holidays_list
        else:
            get_holidays.cache_clear()
            if retry <= 3:
                return get_holidays(retry = retry+1)
        

def isHoliday(
        date_string: DateFormat, 
        segment: Literal["Commodity Morning", "F&O", "Commodity", "Currency", "Equity", "Clearing", "all"]= "all"
        ) -> bool:
    holiday_data = get_holidays()
    exclusion_list = ['holiday_date', 'holiday_day', 'holiday_name']
    
    for holiday in holiday_data:
        if holiday['holiday_date'] == date_string:
            if segment == "all":
                return {segment: holiday[segment] for segment in holiday if segment not in exclusion_list}
            else:
                return holiday[segment]
    if segment == "all":
        return {segment: False for segment in holiday_data[0] if segment not in exclusion_list}
    else:
        return False

