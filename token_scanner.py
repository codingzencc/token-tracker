#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Author: Chaitanya
Email: chaitanya.dec94@gmail.com

A script to fetch if a public key for a wallet has any history of transactions.
A wallet with a single transaction is ALIVE.
A wallet for which there are no transactions is DEAD.
Using covalenthq APIs to fetch the details of various chains

Usage:
python token_scanner.py --input_path="address.txt" --mode="async"
"""
import os
import json
import argparse
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import aiohttp
import asyncio
import requests
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint

chain_id_map = {"1": "ETH", "56": "BSC", "137": "MATIC"}
total_transaction_map = {"ETH": 0, "BSC": 0, "MATIC": 0}
total_quotes_map = {"ETH": 0, "BSC": 0, "MATIC": 0}
split_list = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
SPLIT_SIZE = 5


def get_google_sheet(name="token-tracker-db", sheet_index=0):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    # Assign credentials ann path of style sheet
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json.sample", scope)
    client = gspread.authorize(creds)
    # sheet = client.create("token-tracker-db")
    # sheet = client.open("token-tracker-db")
    sheet = client.open(name).get_worksheet(sheet_index)
    # Inserting data
    # insertRow = ["address", "total_transactions", "sum_of_quotes", "tags"]
    # sheet.insert_row(insertRow, 1)
    # print(sheet.row_values(1))
    return sheet


def modify_results_for_contract_address_summary_and_push_to_gsheet(output_path, input_path, contract_addresses_result, sheet):
    output_path_contract_addresses = output_path + "_contract_address_scan_output" + f"---{output_type}---{input_path}.json"
    out_file_contract_addresses = open(output_path_contract_addresses, "w")
    json.dump(contract_addresses_result, out_file_contract_addresses, indent=4)
    modified_results = {}
    for result in contract_addresses_result:
        address, ticker = list(result.keys())[0].split(":")
        if address not in modified_results:
            modified_results[address] = [total_transaction_map.copy(), total_quotes_map.copy()]
        modified_results[address][0][ticker] = result[list(result.keys())[0]]["transactions"]
        modified_results[address][1][ticker] = result[list(result.keys())[0]]["quote_sum"]
    output_path_modified_contract_addresses = output_path + "_modified_contract_address_scan_output" + f"---{output_type}---{input_path}.json"
    out_file_modified_contract_addresses = open(output_path_modified_contract_addresses, "w")
    json.dump(modified_results, out_file_modified_contract_addresses, indent=4)
    print(output_path_modified_contract_addresses)
    final_result = {"address": [], "total_transactions": [], "sum_of_quotes": []}
    for key, value in modified_results.items():
        final_result["address"].append(key)
        final_result["total_transactions"].append(str(list(value)[0]))
        final_result["sum_of_quotes"].append(str(list(value)[1]))
    result_list = pd.DataFrame(final_result).values.tolist()
    sheet.append_rows(result_list)
    print("Results added to google sheet. Please check!")
    print(json.dumps(modified_results, indent=4))


def sync_get_token_data(addresses, chain_ids, covalent_key):
    data = []
    for chain_id in tqdm(range(len(chain_ids))):
        for address in tqdm(range(len(addresses)), leave=False):
            url = f'https://api.covalenthq.com/v1/{chain_ids[chain_id]}/address/{addresses[address]}/transactions_v2/?quote-currency=USD&format=JSON&block-signed-at-asc=false&no-logs=false&page-number=1&page-size=1&key={covalent_key}'
            response = requests.get(url)
            result_data = response.json()
            items_len = len(result_data['data']['items'])
            remarks = "DEAD" if items_len == 0 else "ALIVE"
            data.append([addresses[address], chain_id_map[chain_ids[chain_id]], remarks])
    return data


async def async_main(addresses, chain_ids, covalent_key, output_type):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for chain_id in chain_ids:
            for address in addresses:
                if output_type == "wallet_status":
                    task = asyncio.ensure_future(get_token_data(session, chain_id, address, covalent_key))
                    tasks.append(task)
                elif output_type == "contract_addresses":
                    task = asyncio.ensure_future(get_contract_addresses(session, chain_id, address, covalent_key))
                    tasks.append(task)
        data = await asyncio.gather(*tasks)
        return data


async def get_token_data(session, chain_id, address, covalent_key):
    url = f'https://api.covalenthq.com/v1/{chain_id}/address/{address}/transactions_v2/?quote-currency=USD&format=JSON&block-signed-at-asc=false&no-logs=false&page-number=1&page-size=1&key={covalent_key}'
    async with session.get(url) as response:
        result_data = await response.json()
        items_len = len(result_data['data']['items'])
        remarks = "DEAD" if items_len == 0 else "ALIVE"
        return [address, chain_id_map[chain_id], remarks]


async def get_contract_addresses(session, chain_id, address, covalent_key):
    url = f'https://api.covalenthq.com/v1/{chain_id}/address/{address}/balances_v2/?quote-currency=USD&format=JSON&nft=false&no-nft-fetch=false&key={covalent_key}'
    async with session.get(url) as response:
        result_data = await response.json()
        data = result_data['data']
        items = [] if 'items' not in data else data['items']
        items_result = {"transactions": len(items), "quote_sum": 0}
        for item in items:
            items_result["quote_sum"] += item["quote"]
        return {address + ":" + chain_id_map[chain_id]: items_result}


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', help='Mode of operation: sync or async. Run as async for faster output', type=str, default="async")
    parser.add_argument('--input_path', help='Filepath to input public key addresses', type=str, required=True)
    parser.add_argument('--output_path', help='Filepath to output token data', type=str, default=datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))
    parser.add_argument('--output_type', help='Type of output needed: wallet_status/contract_addresses', type=str, default="contract_addresses")
    parser.add_argument('--chain_ids', help='Comma separated Chain IDs for Etherum chains - defaults for ETH, BSC, MATIC', type=str, default="1,56,137")
    parser.add_argument('--covalent_key', help='Covalent HQ API key', type=str, default="ckey_d54b0f39f601441c9b43544a3a9")
    args = parser.parse_args()
    input_path = args.input_path
    if not os.path.exists(input_path):
        print("not a valid input file!")
        exit(0)
    mode = args.mode
    input_addresses = set(open(input_path, "r").read().splitlines())
    sheet = get_google_sheet()
    existing_addresses = set(sheet.col_values(1)[1:])
    duplicate_addresses = input_addresses & existing_addresses
    addresses = list(input_addresses - duplicate_addresses)
    print("skipping following addresses as they are already processed:")
    for duplicate_address in duplicate_addresses:
        print(duplicate_address)
    output_type = args.output_type
    output_path = args.output_path
    chain_ids = args.chain_ids.split(",")
    covalent_key = args.covalent_key
    wallet_status_result = []
    if mode == "sync":
        wallet_status_result = sync_get_token_data(addresses, chain_ids, covalent_key)
    elif mode == "async":
        split_addresses = split_list(addresses, SPLIT_SIZE)
        for split_address in split_addresses:
            wallet_status_result.extend(asyncio.run(async_main(split_address, chain_ids, covalent_key, "wallet_status")))
    else:
        print("Invalid Mode")
        exit(0)
    if output_type == "wallet_status":
        fieldnames = ['Address', 'Chain', 'Remarks']
        df = pd.DataFrame(data=wallet_status_result)
        df.columns = fieldnames
        output_path_wallet_status = output_path + "_address_transaction_scan_output" + f"---{output_type}---{input_path}.csv"
        df.to_csv(output_path, index=False)
        print(output_path_wallet_status)
    elif output_type == "contract_addresses":
        contract_addresses_result = []
        alive_wallets_addresses = list(set([x[0] for x in wallet_status_result if x[2] == "ALIVE"]))
        split_alive_wallets_addresses = split_list(alive_wallets_addresses, SPLIT_SIZE)
        for split_alive_wallets_address in split_alive_wallets_addresses:
            contract_addresses_result.extend(asyncio.run(async_main(split_alive_wallets_address, chain_ids, covalent_key, output_type)))
        modify_results_for_contract_address_summary_and_push_to_gsheet(output_path, input_path, contract_addresses_result, sheet)
