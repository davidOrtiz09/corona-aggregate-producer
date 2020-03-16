
from csv import DictWriter, DictReader
from time import gmtime, strftime
from typing import List


field_names = [
    'Province/State',
    'Country/Region',
    'Lat',
    'Long'
]

input_file_path = 'resources/'
output_file_path = 'outputs/'

def get_headers(file_name: str) -> List[str]:
    output_date_names = []
    with open(f'{input_file_path}{file_name}', 'r') as in_confirmed_cases:
        confirmed_cases_reader = DictReader(in_confirmed_cases)
        for xxx in confirmed_cases_reader:
            headers = list(xxx.keys())
            total_lenght = len(headers)
            output_date_names = headers[4:total_lenght]
            break
    return output_date_names    


def collect_and_aggregate_by_file(file_name: str) -> dict:
    print(f"Procesando archivo: {file_name} .....")
    output_date_names = get_headers(file_name)
    output_field_names = field_names + output_date_names  
    raw_dates = {}
    agreggate_dates = {}
    with open(f'{input_file_path}{file_name}', 'r') as in_confirmed_cases:
                confirmed_cases_reader = DictReader(
                    in_confirmed_cases, fieldnames=output_field_names)
                next(confirmed_cases_reader, None)
                for confirmed_case in confirmed_cases_reader:
                    if (confirmed_case['Country/Region'] == 'US'):
                        for current_date in output_date_names:
                            if (confirmed_case[current_date] != ''):
                                if (raw_dates.get(current_date) == None):
                                    raw_dates[current_date] = [int(confirmed_case[current_date])]
                                else :
                                    raw_dates[current_date].append(int(confirmed_case[current_date]))
                for raw_date in raw_dates:
                    agreggate_dates[raw_date] = sum(raw_dates[raw_date])
    print(f"Consolidaciòn archivo : {file_name} finalizada !")            
    return agreggate_dates


def produce_new_file(file_name: str, aggregate_by_date: dict) -> None:
    print(f"Creando nuevo archivo consolidado: {file_name} .....")
    output_date_names = get_headers(file_name)
    with open(f'{output_file_path}usa-{file_name}', 'w') as out_aggregates:
        outwriter = DictWriter(out_aggregates, fieldnames=output_date_names)
        outwriter.writeheader()
        outwriter.writerow(aggregate_by_date)





if __name__ == '__main__':
    print("Leyendo archivos ......")
    agreggate_confirmed_dates = collect_and_aggregate_by_file('time_series_19-covid-Confirmed.csv')
    agreggate_deaths_dates = collect_and_aggregate_by_file('time_series_19-covid-Deaths.csv')
    agreggate_recovered_dates = collect_and_aggregate_by_file('time_series_19-covid-Recovered.csv')

    produce_new_file('time_series_19-covid-Confirmed.csv', agreggate_confirmed_dates)
    produce_new_file('time_series_19-covid-Deaths.csv', agreggate_confirmed_dates)
    produce_new_file('time_series_19-covid-Recovered.csv', agreggate_confirmed_dates)



