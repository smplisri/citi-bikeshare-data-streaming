import boto3
import json
from datetime import datetime, timedelta
import argparse
import logging
from scripts.utils import data_fetcher, url_selector, data_driller

def cli():
    input_data_api_url = "http://gbfs.citibikenyc.com/gbfs/gbfs.json"
    client = boto3.client('kinesis')

    parser = argparse.ArgumentParser(
        description="Supply the required parameters as input to the script",
        prog="main.py"
    )

    language_choices = [ item for item in url_selector(input_data_api_url)["data"].keys() ]

    parser.add_argument(
        '-l', '--lang', action='store', required=True, dest='lang',
        choices=language_choices
    )

    argument_choices = [ item["name"] for item in url_selector(input_data_api_url)["data"][language_choices[0]]["feeds"][:] ]

    parser.add_argument(
        '-o', '--option', action='store', required=True, dest='option',
        choices=argument_choices
    )

    parser.add_argument(
        '-s', '--stream', action='store', required=True, dest='stream'
    )

    parser.add_argument(
        '-p', '--partition_key', action='store', required=True, dest='part'
    )

    parser.add_argument(
        '-hi', '--hierarchy', action='store', dest='hierarchy'
    )

    args = parser.parse_args()

    required_url = url_selector(input_data_api_url, lang=args.lang, option=args.option)
    print("Trying to reach the following url in loops hereafter for information -- " + required_url)
    iterator = 1

    while True:
        output, current_timer, ttl = data_fetcher(required_url)
        finalized_data = data_driller(output, args.hierarchy.split(","))
        for record in finalized_data:
            client.put_record(
                Data = json.dumps(record).encode('utf-8'),
                PartitionKey = args.part,
                StreamName = args.stream
            )
        print("Iteration number - {}: Sent {} number of records to the kinesis data stream".format(str(iterator), str(len(finalized_data))))
        iterator = iterator + 1
        if datetime.now() < current_timer + timedelta(seconds=ttl):
            sleep(ttl)
        else:
            continue

if __name__ == '__main__':
    cli()