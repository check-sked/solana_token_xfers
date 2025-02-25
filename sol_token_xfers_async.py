import json
import time
import datetime
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter

# Rate limiter: 300 requests per second
RATE_LIMIT = 300
rate_limiter = AsyncLimiter(RATE_LIMIT, 1)

RPC_ENDPOINT = "https://solana-mainnet.g.alchemy.com/v2/AMsnqGqzMboS_tNkYDeec0MleUfhykIR"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_INSTRUCTION_TYPES = {"transfer", "transferChecked", "mintTo", "mintToChecked", "burn", "burnChecked"}

WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
KNOWN_MINTS = {
    WRAPPED_SOL_MINT: {"decimals": 9}
}

account_info_cache = {}
mint_info_cache = {}

async def fetch_solana_block(session, block_number: int) -> dict:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            block_number,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "transactionDetails": "full",
                "rewards": False
            }
        ]
    }
    headers = {"Content-Type": "application/json"}
    
    async with rate_limiter:
        try:
            async with session.post(RPC_ENDPOINT, headers=headers, json=payload, ssl=False) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            print(f"Error fetching block {block_number}: {str(e)}")
            return None

async def fetch_account_info(session, account_pubkey: str, retry_count=3) -> dict:
    if account_pubkey in account_info_cache:
        return account_info_cache[account_pubkey]
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            account_pubkey,
            {"encoding": "jsonParsed"}
        ]
    }
    headers = {"Content-Type": "application/json"}
    
    for attempt in range(retry_count):
        async with rate_limiter:
            try:
                async with session.post(RPC_ENDPOINT, headers=headers, json=payload, ssl=False) as response:
                    response.raise_for_status()
                    data = await response.json()
                    value = data.get("result", {}).get("value")
                    account_info_cache[account_pubkey] = value
                    return value
            except Exception as e:
                if attempt < retry_count - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    
    return None

async def fetch_mint_info(session, mint_pubkey: str) -> dict:
    if mint_pubkey in mint_info_cache:
        return mint_info_cache[mint_pubkey]
    
    info = await fetch_account_info(session, mint_pubkey)
    if not info:
        return {}
    
    try:
        parsed_info = info.get("data", {}).get("parsed", {}).get("info", {})
        result = {
            "decimals": parsed_info.get("decimals"),
            "mintAuthority": parsed_info.get("mintAuthority"),
            "supply": parsed_info.get("supply")
        }
        mint_info_cache[mint_pubkey] = result
        return result
    except Exception as e:
        return {}

async def get_token_account_details(session, account: str) -> dict:
    if not account:
        return {}
        
    info = await fetch_account_info(session, account)
    if not info:
        return {}
    
    try:
        if info.get("owner") != SPL_TOKEN_PROGRAM_ID:
            return {}
            
        parsed_info = info.get("data", {}).get("parsed", {}).get("info", {})
        mint = parsed_info.get("mint")
        owner = parsed_info.get("owner")
        
        result = {
            "owner": owner,
            "mint": mint,
            "decimals": parsed_info.get("tokenAmount", {}).get("decimals")
        }
        
        if result["decimals"] is None and mint:
            mint_info = await fetch_mint_info(session, mint)
            result["decimals"] = mint_info.get("decimals")
            
        return result
    except Exception as e:
        return {}

async def resolve_token_account_owner(session, token_account: str) -> str:
    if not token_account:
        return token_account
        
    details = await get_token_account_details(session, token_account)
    if details and details.get("owner"):
        return details["owner"]
    
    return token_account

async def is_token_account(session, account: str) -> bool:
    if not account:
        return False
        
    info = await fetch_account_info(session, account)
    if not info:
        return False
        
    return info.get("owner") == SPL_TOKEN_PROGRAM_ID

def format_block_time(timestamp):
    """Convert Unix timestamp to human-readable format"""
    if timestamp is None:
        return None
    dt = datetime.datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

async def extract_token_transfers(session, tx_json: dict, block_number: int, block_time: int) -> list:
    transfers = []
    signature = tx_json.get("transaction", {}).get("signatures", [None])[0]
    
    meta_status = tx_json.get("meta", {}).get("status", {})
    tx_status = "failed" if "Err" in meta_status else "succeeded"

    instructions = []
    message = tx_json.get("transaction", {}).get("message", {})
    if "instructions" in message:
        instructions.extend(message["instructions"])
    meta = tx_json.get("meta", {})
    if "instructions" in meta:
        instructions.extend(meta["instructions"])
    if "innerInstructions" in meta:
        for inner in meta["innerInstructions"]:
            instructions.extend(inner.get("instructions", []))

    all_accounts = tx_json.get("transaction", {}).get("message", {}).get("accountKeys", [])
    account_pubkeys = [account.get("pubkey") for account in all_accounts if account.get("pubkey")]
    
    token_account_info = {}
    for balance in meta.get("preTokenBalances", []) + meta.get("postTokenBalances", []):
        account_idx = balance.get("accountIndex")
        if account_idx is not None and 0 <= account_idx < len(account_pubkeys):
            account_pubkey = account_pubkeys[account_idx]
            token_account_info[account_pubkey] = {
                "mint": balance.get("mint"),
                "owner": balance.get("owner"),
                "decimals": balance.get("uiTokenAmount", {}).get("decimals")
            }

    for instr in instructions:
        if instr.get("program") == "spl-token" and instr.get("programId") == SPL_TOKEN_PROGRAM_ID:
            if "parsed" in instr:
                parsed = instr["parsed"]
                instr_type = parsed.get("type")
                if instr_type in TOKEN_INSTRUCTION_TYPES:
                    info = parsed.get("info", {})
                    
                    source_token_account = info.get("source")
                    destination_token_account = info.get("destination")
                    authority = info.get("authority")
                    mint = info.get("mint")
                    amount = info.get("amount")
                    decimals = info.get("decimals")
                    
                    # Initialize transfer object with default values
                    transfer = {
                        "signature": signature,
                        "type": instr_type,
                        "source": None,
                        "source_token_account": source_token_account,
                        "destination": None,
                        "destination_token_account": destination_token_account,
                        "mint": mint,
                        "amount": amount,
                        "decimals": decimals,
                        "tx_status": tx_status,
                        "block_id": block_number,
                        "block_time": format_block_time(block_time)
                    }
                    
                    if instr_type in ("transfer", "transferChecked"):
                        if authority:
                            transfer["source"] = authority
                        
                    elif instr_type in ("burn", "burnChecked"):
                        if not transfer["source_token_account"] and "account" in info:
                            transfer["source_token_account"] = info.get("account")
                        
                        if authority:
                            transfer["source"] = authority
                    
                    elif instr_type in ("mintTo", "mintToChecked"):
                        if "mintAuthority" in info:
                            transfer["source"] = info.get("mintAuthority")
                        
                        if not transfer["destination_token_account"] and "account" in info:
                            transfer["destination_token_account"] = info.get("account")
                    
                    if (not transfer["amount"] or not transfer["decimals"]) and "tokenAmount" in info:
                        token_amount = info["tokenAmount"]
                        if not transfer["amount"]:
                            transfer["amount"] = token_amount.get("amount")
                        if not transfer["decimals"]:
                            transfer["decimals"] = token_amount.get("decimals")
                    
                    if transfer["source_token_account"]:
                        account_info = token_account_info.get(transfer["source_token_account"], {})
                        if account_info and account_info.get("owner") and not transfer["source"]:
                            transfer["source"] = account_info["owner"]
                        if account_info and account_info.get("mint") and not transfer["mint"]:
                            transfer["mint"] = account_info["mint"]
                        if account_info and account_info.get("decimals") and not transfer["decimals"]:
                            transfer["decimals"] = account_info["decimals"]
                        
                        if not transfer["source"] or not transfer["mint"] or not transfer["decimals"]:
                            if await is_token_account(session, transfer["source_token_account"]):
                                source_details = await get_token_account_details(session, transfer["source_token_account"])
                                if source_details:
                                    if source_details.get("owner") and not transfer["source"]:
                                        transfer["source"] = source_details["owner"]
                                    if source_details.get("mint") and not transfer["mint"]:
                                        transfer["mint"] = source_details["mint"]
                                    if source_details.get("decimals") and not transfer["decimals"]:
                                        transfer["decimals"] = source_details["decimals"]
                    
                    if transfer["destination_token_account"]:
                        account_info = token_account_info.get(transfer["destination_token_account"], {})
                        if account_info and account_info.get("owner") and not transfer["destination"]:
                            transfer["destination"] = account_info["owner"]
                        if account_info and account_info.get("mint") and not transfer["mint"]:
                            transfer["mint"] = account_info["mint"]
                        if account_info and account_info.get("decimals") and not transfer["decimals"]:
                            transfer["decimals"] = account_info["decimals"]
                        
                        if not transfer["destination"] or not transfer["mint"] or not transfer["decimals"]:
                            if await is_token_account(session, transfer["destination_token_account"]):
                                dest_details = await get_token_account_details(session, transfer["destination_token_account"])
                                if dest_details:
                                    if dest_details.get("owner") and not transfer["destination"]:
                                        transfer["destination"] = dest_details["owner"]
                                    if dest_details.get("mint") and not transfer["mint"]:
                                        transfer["mint"] = dest_details["mint"]
                                    if dest_details.get("decimals") and not transfer["decimals"]:
                                        transfer["decimals"] = dest_details["decimals"]
                    
                    if not transfer["mint"]:
                        source_acc_info = await fetch_account_info(session, transfer["source_token_account"]) if transfer["source_token_account"] else None
                        dest_acc_info = await fetch_account_info(session, transfer["destination_token_account"]) if transfer["destination_token_account"] else None
                        
                        if (source_acc_info and source_acc_info.get("data", {}).get("parsed", {}).get("info", {}).get("mint") == WRAPPED_SOL_MINT) or \
                           (dest_acc_info and dest_acc_info.get("data", {}).get("parsed", {}).get("info", {}).get("mint") == WRAPPED_SOL_MINT):
                            transfer["mint"] = WRAPPED_SOL_MINT
                            if not transfer["decimals"]:
                                transfer["decimals"] = KNOWN_MINTS[WRAPPED_SOL_MINT]["decimals"]
                    
                    if transfer["mint"] and not transfer["decimals"]:
                        if transfer["mint"] in KNOWN_MINTS:
                            transfer["decimals"] = KNOWN_MINTS[transfer["mint"]]["decimals"]
                        else:
                            mint_info = await fetch_mint_info(session, transfer["mint"])
                            if mint_info and mint_info.get("decimals") is not None:
                                transfer["decimals"] = mint_info["decimals"]
                    
                    if not transfer["mint"]:
                        print(f"Warning: Using wSOL as default for missing mint in {instr_type} tx {signature}")
                        transfer["mint"] = WRAPPED_SOL_MINT
                    
                    if not transfer["decimals"] and instr_type in ("burn", "burnChecked"):
                        transfer["decimals"] = 0
                    elif not transfer["decimals"]:
                        print(f"Warning: Using wSOL decimals as default for missing decimals in {instr_type} tx {signature}")
                        transfer["decimals"] = KNOWN_MINTS[WRAPPED_SOL_MINT]["decimals"]
                    
                    if not transfer["source"] and transfer["source_token_account"]:
                        print(f"Warning: Could not determine source for {instr_type} tx {signature}, using token account as fallback")
                        transfer["source"] = transfer["source_token_account"]
                    
                    if not transfer["destination"] and transfer["destination_token_account"]:
                        if tx_status == "succeeded":
                            print(f"Warning: Could not determine destination for {instr_type} tx {signature}, using token account as fallback")
                        transfer["destination"] = transfer["destination_token_account"]
                    
                    if instr_type in ("burn", "burnChecked") and not transfer["destination"]:
                        transfer["destination"] = "BURN"
                        transfer["destination_token_account"] = "BURN"
                    
                    if instr_type in ("mintTo", "mintToChecked") and not transfer["source"]:
                        if transfer["mint"]:
                            transfer["source"] = f"MINT:{transfer['mint']}"
                        else:
                            transfer["source"] = "MINT"
                    
                    transfers.append(transfer)
    
    return transfers

async def process_transaction(session, tx, block_number, block_time):
    return await extract_token_transfers(session, tx, block_number, block_time)

async def process_all_transactions(transactions, block_number, block_time):
    async with aiohttp.ClientSession() as session:
        tasks = [process_transaction(session, tx, block_number, block_time) for tx in transactions]
        results = await asyncio.gather(*tasks)
        
        all_transfers = []
        for result in results:
            if result:
                all_transfers.extend(result)
                
        return all_transfers

async def main_async():
    block_number = 322800403
    
    async with aiohttp.ClientSession() as session:
        block_data = await fetch_solana_block(session, block_number)
        if not block_data or "result" not in block_data:
            print("Failed to fetch block data")
            return

        block_time = block_data["result"].get("blockTime")
        transactions = block_data["result"].get("transactions", [])
        
        print(f"Processing {len(transactions)} transactions from block {block_number}")
        start_time = time.time()
        
        all_transfers = await process_all_transactions(transactions, block_number, block_time)
        
        end_time = time.time()
        print(f"Processed {len(transactions)} transactions in {end_time - start_time:.2f} seconds")
        print(f"Found {len(all_transfers)} token transfers")

        if all_transfers:
            with open("token_transfers.json", "w") as outfile:
                json.dump(all_transfers, outfile, indent=2)
            print(f"Results written to token_transfers.json")

def main():
    asyncio.run(main_async())
    
if __name__ == "__main__":
    main()