"""
Solana Token Transfer Extractor
------------------------------
Extracts SPL token transfers from Solana blockchain blocks - includes instruction associated w/ transfer.
"""
import json
import time
import datetime
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RPC_ENDPOINT = "https://solana-mainnet.g.alchemy.com/v2/AMsnqGqzMboS_tNkYDeec0MleUfhykIR"
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_INSTRUCTION_TYPES = {"transfer", "transferChecked", "mintTo", "mintToChecked", "burn", "burnChecked"}

# Instructions to ignore
IGNORE_HIGH_LEVEL = {
    "getaccountdatasize",
    "initializeimmutableowner",
    "create",
    "createaccount",
    "closeaccount"
}

# Rate limiter
RATE_LIMIT = 300
rate_limiter = AsyncLimiter(RATE_LIMIT, 1)

# Cache structures
KNOWN_MINTS = {
    WRAPPED_SOL_MINT: {"decimals": 9}
}

@dataclass
class TokenTransfer:
    """Represents a token transfer on Solana"""
    signature: str
    type: str
    source: Optional[str] = None
    source_token_account: Optional[str] = None
    destination: Optional[str] = None
    destination_token_account: Optional[str] = None
    mint: Optional[str] = None
    amount: Optional[str] = None
    decimals: Optional[int] = None
    tx_status: str = "succeeded"
    block_id: Optional[int] = None
    block_time: Optional[str] = None
    instruction: Optional[str] = None
    instruction_address: Optional[str] = None

class AccountCache:
    """Cache for account and mint information"""
    def __init__(self):
        self.account_info_cache: Dict[str, Any] = {}
        self.mint_info_cache: Dict[str, Dict] = {}
        self.tx_account_owners: Dict[str, Dict[str, str]] = {}

    def get_account_info(self, account_pubkey: str) -> Optional[Any]:
        return self.account_info_cache.get(account_pubkey)

    def set_account_info(self, account_pubkey: str, info: Any) -> None:
        self.account_info_cache[account_pubkey] = info

    def get_mint_info(self, mint_pubkey: str) -> Optional[Dict]:
        return self.mint_info_cache.get(mint_pubkey)

    def set_mint_info(self, mint_pubkey: str, info: Dict) -> None:
        self.mint_info_cache[mint_pubkey] = info
        
    def register_account_in_tx(self, tx_sig: str, account: str, owner: str, mint: Optional[str] = None) -> None:
        if tx_sig not in self.tx_account_owners:
            self.tx_account_owners[tx_sig] = {}
        self.tx_account_owners[tx_sig][account] = {"owner": owner, "mint": mint}
        
    def get_tx_account_info(self, tx_sig: str, account: str) -> Optional[Dict]:
        tx_accounts = self.tx_account_owners.get(tx_sig, {})
        return tx_accounts.get(account)

class SolanaRpcClient:
    """Client for interacting with Solana RPC"""
    def __init__(self, endpoint: str, cache: AccountCache):
        self.endpoint = endpoint
        self.cache = cache

    async def fetch_solana_block(self, session: aiohttp.ClientSession, block_number: int) -> Optional[Dict]:
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
                async with session.post(self.endpoint, headers=headers, json=payload, ssl=False) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                logger.error(f"Error fetching block {block_number}: {str(e)}")
                return None

    async def fetch_account_info(self, session: aiohttp.ClientSession, account_pubkey: str, retry_count=3) -> Optional[Dict]:
        cached_info = self.cache.get_account_info(account_pubkey)
        if cached_info is not None:
            return cached_info
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [account_pubkey, {"encoding": "jsonParsed"}]
        }
        headers = {"Content-Type": "application/json"}
        for attempt in range(retry_count):
            async with rate_limiter:
                try:
                    async with session.post(self.endpoint, headers=headers, json=payload, ssl=False) as response:
                        response.raise_for_status()
                        data = await response.json()
                        value = data.get("result", {}).get("value")
                        self.cache.set_account_info(account_pubkey, value)
                        return value
                except Exception as e:
                    if attempt < retry_count - 1:
                        await asyncio.sleep(0.5 * (2 ** attempt))
        return None

    async def fetch_mint_info(self, session: aiohttp.ClientSession, mint_pubkey: str) -> Dict:
        cached_info = self.cache.get_mint_info(mint_pubkey)
        if cached_info is not None:
            return cached_info
        info = await self.fetch_account_info(session, mint_pubkey)
        if not info:
            return {}
        try:
            parsed_info = info.get("data", {}).get("parsed", {}).get("info", {})
            result = {
                "decimals": parsed_info.get("decimals"),
                "mintAuthority": parsed_info.get("mintAuthority"),
                "supply": parsed_info.get("supply")
            }
            self.cache.set_mint_info(mint_pubkey, result)
            return result
        except Exception:
            return {}

class TokenAccountProcessor:
    """Process token accounts and transactions"""
    def __init__(self, rpc_client: SolanaRpcClient):
        self.rpc_client = rpc_client

    async def get_token_account_details(self, session: aiohttp.ClientSession, account: str) -> Dict:
        if not account:
            return {}
        info = await self.rpc_client.fetch_account_info(session, account)
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
                mint_info = await self.rpc_client.fetch_mint_info(session, mint)
                result["decimals"] = mint_info.get("decimals")
            return result
        except Exception:
            return {}

    async def is_token_account(self, session: aiohttp.ClientSession, account: str) -> bool:
        if not account:
            return False
        info = await self.rpc_client.fetch_account_info(session, account)
        if not info:
            return False
        return info.get("owner") == SPL_TOKEN_PROGRAM_ID

class InstructionParser:
    """Parse Solana transaction instructions to extract token account ownership information"""
    def __init__(self, cache: AccountCache):
        self.cache = cache
        
    def parse_instructions(self, tx_json: Dict, signature: str) -> None:
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
        #account_pubkeys = [account.get("pubkey") for account in all_accounts if account.get("pubkey")]
        for instr in instructions:
            if (instr.get("program") == "spl-token" and 
                instr.get("programId") == SPL_TOKEN_PROGRAM_ID and 
                "parsed" in instr):
                parsed = instr["parsed"]
                instr_type = parsed.get("type", "")
                info = parsed.get("info", {})
                if instr_type in ["initializeAccount", "initializeAccount2", "initializeAccount3"]:
                    account = info.get("account")
                    owner = info.get("owner")
                    mint = info.get("mint")
                    if account and owner:
                        self.cache.register_account_in_tx(signature, account, owner, mint)
                elif instr_type == "closeAccount":
                    account = info.get("account")
                    owner = info.get("owner")
                    if account and owner:
                        self.cache.register_account_in_tx(signature, account, owner)

class TokenTransferExtractor:
    """Extract token transfers from Solana transactions"""
    def __init__(self, rpc_client: SolanaRpcClient, token_processor: TokenAccountProcessor, instruction_parser: InstructionParser):
        self.rpc_client = rpc_client
        self.token_processor = token_processor
        self.instruction_parser = instruction_parser
        self.cache = rpc_client.cache

    @staticmethod
    def format_block_time(timestamp):
        if timestamp is None:
            return None
        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    async def extract_token_transfers(self, session: aiohttp.ClientSession, tx_json: Dict, block_number: int, block_time: int) -> List[TokenTransfer]:
        transfers = []
        signature = tx_json.get("transaction", {}).get("signatures", [None])[0]
        meta_status = tx_json.get("meta", {}).get("status", {})
        tx_status = "failed" if "Err" in meta_status else "succeeded"

        self.instruction_parser.parse_instructions(tx_json, signature)

        all_accounts = tx_json.get("transaction", {}).get("message", {}).get("accountKeys", [])
        account_pubkeys = [account.get("pubkey") for account in all_accounts if account.get("pubkey")]
        token_account_info = {}
        for balance in tx_json.get("meta", {}).get("preTokenBalances", []) + tx_json.get("meta", {}).get("postTokenBalances", []):
            account_idx = balance.get("accountIndex")
            if account_idx is not None and 0 <= account_idx < len(account_pubkeys):
                account_pubkey = account_pubkeys[account_idx]
                token_account_info[account_pubkey] = {
                    "mint": balance.get("mint"),
                    "owner": balance.get("owner"),
                    "decimals": balance.get("uiTokenAmount", {}).get("decimals")
                }
                if balance.get("owner"):
                    self.cache.register_account_in_tx(signature, account_pubkey, balance.get("owner"), balance.get("mint"))

        meta = tx_json.get("meta", {})
        message = tx_json.get("transaction", {}).get("message", {})

        # --- Extract high-level instruction names and addresses from log messages ---
        non_token_instrs = []
        log_messages = meta.get("logMessages", [])
        for i, log in enumerate(log_messages):
            if log.startswith("Program log: Instruction:"):
                instr_name = log.split("Program log: Instruction:")[1].strip()
                if instr_name.lower() in IGNORE_HIGH_LEVEL:
                    continue
                if instr_name.lower() not in {x.lower() for x in TOKEN_INSTRUCTION_TYPES}:
                    instruction_address = None
                    if i > 0:
                        prev_log = log_messages[i - 1]
                        if prev_log.startswith("Program ") and "invoke" in prev_log:
                            parts = prev_log.split()
                            if len(parts) >= 2:
                                instruction_address = parts[1]
                    non_token_instrs.append((instr_name, instruction_address))
        
        # --- Process inner instructions by group ---
        inner_transfers = []
        if "innerInstructions" in meta:
            for group in meta["innerInstructions"]:
                group_instructions = group.get("instructions", [])
                group_transfers = []
                for instr in group_instructions:
                    if instr.get("program") == "spl-token" and instr.get("programId") == SPL_TOKEN_PROGRAM_ID and "parsed" in instr:
                        parsed = instr["parsed"]
                        instr_type = parsed.get("type")
                        if instr_type in TOKEN_INSTRUCTION_TYPES:
                            info = parsed.get("info", {})
                            transfer = TokenTransfer(
                                signature=signature,
                                type=instr_type,
                                source_token_account=info.get("source"),
                                destination_token_account=info.get("destination"),
                                mint=info.get("mint"),
                                amount=info.get("amount"),
                                decimals=info.get("decimals"),
                                tx_status=tx_status,
                                block_id=block_number,
                                block_time=self.format_block_time(block_time),
                                instruction=None,
                                instruction_address=None
                            )
                            if instr_type in ("transfer", "transferChecked"):
                                if authority := info.get("authority"):
                                    transfer.source = authority
                            elif instr_type in ("burn", "burnChecked"):
                                if not transfer.source_token_account and "account" in info:
                                    transfer.source_token_account = info.get("account")
                                if authority := info.get("authority"):
                                    transfer.source = authority
                            elif instr_type in ("mintTo", "mintToChecked"):
                                if "mintAuthority" in info:
                                    transfer.source = info.get("mintAuthority")
                                if not transfer.destination_token_account and "account" in info:
                                    transfer.destination_token_account = info.get("account")
                            if (not transfer.amount or not transfer.decimals) and "tokenAmount" in info:
                                token_amount = info["tokenAmount"]
                                if not transfer.amount:
                                    transfer.amount = token_amount.get("amount")
                                if not transfer.decimals:
                                    transfer.decimals = token_amount.get("decimals")
                            group_transfers.append(transfer)
                if group_transfers:
                    if non_token_instrs:
                        high_level_instr, instr_addr = non_token_instrs.pop(0)
                    else:
                        high_level_instr, instr_addr = None, None
                    for transfer in group_transfers:
                        transfer.instruction = high_level_instr
                        transfer.instruction_address = instr_addr
                        await self._resolve_source_details(session, transfer, token_account_info, signature)
                        await self._resolve_destination_details(session, transfer, token_account_info, signature)
                        await self._apply_defaults(session, transfer, signature, transfer.type)
                    inner_transfers.extend(group_transfers)
        
        # --- Process outer instructions (without high-level mapping) ---
        outer_instructions = []
        if "instructions" in message:
            outer_instructions.extend(message["instructions"])
        if "instructions" in meta:
            outer_instructions.extend(meta["instructions"])
        outer_transfers = []
        for instr in outer_instructions:
            if instr.get("program") == "spl-token" and instr.get("programId") == SPL_TOKEN_PROGRAM_ID:
                if "parsed" in instr:
                    parsed = instr["parsed"]
                    instr_type = parsed.get("type")
                    if instr_type in TOKEN_INSTRUCTION_TYPES:
                        info = parsed.get("info", {})
                        transfer = TokenTransfer(
                            signature=signature,
                            type=instr_type,
                            source_token_account=info.get("source"),
                            destination_token_account=info.get("destination"),
                            mint=info.get("mint"),
                            amount=info.get("amount"),
                            decimals=info.get("decimals"),
                            tx_status=tx_status,
                            block_id=block_number,
                            block_time=self.format_block_time(block_time),
                            instruction=None,
                            instruction_address=None
                        )
                        if instr_type in ("transfer", "transferChecked"):
                            if authority := info.get("authority"):
                                transfer.source = authority
                        elif instr_type in ("burn", "burnChecked"):
                            if not transfer.source_token_account and "account" in info:
                                transfer.source_token_account = info.get("account")
                            if authority := info.get("authority"):
                                transfer.source = authority
                        elif instr_type in ("mintTo", "mintToChecked"):
                            if "mintAuthority" in info:
                                transfer.source = info.get("mintAuthority")
                            if not transfer.destination_token_account and "account" in info:
                                transfer.destination_token_account = info.get("account")
                        if (not transfer.amount or not transfer.decimals) and "tokenAmount" in info:
                            token_amount = info["tokenAmount"]
                            if not transfer.amount:
                                transfer.amount = token_amount.get("amount")
                            if not transfer.decimals:
                                transfer.decimals = token_amount.get("decimals")
                        await self._resolve_source_details(session, transfer, token_account_info, signature)
                        await self._resolve_destination_details(session, transfer, token_account_info, signature)
                        await self._apply_defaults(session, transfer, signature, instr_type)
                        outer_transfers.append(transfer)
        
        transfers = inner_transfers + outer_transfers
        return transfers

    async def _resolve_source_details(self, session: aiohttp.ClientSession, transfer: TokenTransfer, token_account_info: Dict, signature: str) -> None:
        if transfer.source_token_account:
            account_info = token_account_info.get(transfer.source_token_account, {})
            if account_info:
                if account_info.get("owner") and not transfer.source:
                    transfer.source = account_info["owner"]
                if account_info.get("mint") and not transfer.mint:
                    transfer.mint = account_info["mint"]
                if account_info.get("decimals") and not transfer.decimals:
                    transfer.decimals = account_info["decimals"]
            if not transfer.source or not transfer.mint or not transfer.decimals:
                tx_account_info = self.cache.get_tx_account_info(signature, transfer.source_token_account)
                if tx_account_info:
                    if tx_account_info.get("owner") and not transfer.source:
                        transfer.source = tx_account_info["owner"]
                    if tx_account_info.get("mint") and not transfer.mint:
                        transfer.mint = tx_account_info["mint"]
            if not transfer.source or not transfer.mint or not transfer.decimals:
                if await self.token_processor.is_token_account(session, transfer.source_token_account):
                    source_details = await self.token_processor.get_token_account_details(session, transfer.source_token_account)
                    if source_details:
                        if source_details.get("owner") and not transfer.source:
                            transfer.source = source_details["owner"]
                        if source_details.get("mint") and not transfer.mint:
                            transfer.mint = source_details["mint"]
                        if source_details.get("decimals") and not transfer.decimals:
                            transfer.decimals = source_details["decimals"]

    async def _resolve_destination_details(self, session: aiohttp.ClientSession, transfer: TokenTransfer, token_account_info: Dict, signature: str) -> None:
        if transfer.destination_token_account:
            account_info = token_account_info.get(transfer.destination_token_account, {})
            if account_info:
                if account_info.get("owner") and not transfer.destination:
                    transfer.destination = account_info["owner"]
                if account_info.get("mint") and not transfer.mint:
                    transfer.mint = account_info["mint"]
                if account_info.get("decimals") and not transfer.decimals:
                    transfer.decimals = account_info["decimals"]
            if not transfer.destination or not transfer.mint or not transfer.decimals:
                tx_account_info = self.cache.get_tx_account_info(signature, transfer.destination_token_account)
                if tx_account_info:
                    if tx_account_info.get("owner") and not transfer.destination:
                        transfer.destination = tx_account_info["owner"]
                    if tx_account_info.get("mint") and not transfer.mint:
                        transfer.mint = tx_account_info["mint"]
            if not transfer.destination or not transfer.mint or not transfer.decimals:
                if await self.token_processor.is_token_account(session, transfer.destination_token_account):
                    dest_details = await self.token_processor.get_token_account_details(session, transfer.destination_token_account)
                    if dest_details:
                        if dest_details.get("owner") and not transfer.destination:
                            transfer.destination = dest_details["owner"]
                        if dest_details.get("mint") and not transfer.mint:
                            transfer.mint = dest_details["mint"]
                        if dest_details.get("decimals") and not transfer.decimals:
                            transfer.decimals = dest_details["decimals"]

    async def _apply_defaults(self, session: aiohttp.ClientSession, transfer: TokenTransfer, signature: str, instr_type: str) -> None:
        if not transfer.mint:
            source_acc_info = await self.rpc_client.fetch_account_info(session, transfer.source_token_account) if transfer.source_token_account else None
            dest_acc_info = await self.rpc_client.fetch_account_info(session, transfer.destination_token_account) if transfer.destination_token_account else None
            if (source_acc_info and self._extract_mint_from_account(source_acc_info) == WRAPPED_SOL_MINT) or \
               (dest_acc_info and self._extract_mint_from_account(dest_acc_info) == WRAPPED_SOL_MINT):
                transfer.mint = WRAPPED_SOL_MINT
                if not transfer.decimals:
                    transfer.decimals = KNOWN_MINTS[WRAPPED_SOL_MINT]["decimals"]
        if transfer.mint and not transfer.decimals:
            if transfer.mint in KNOWN_MINTS:
                transfer.decimals = KNOWN_MINTS[transfer.mint]["decimals"]
            else:
                mint_info = await self.rpc_client.fetch_mint_info(session, transfer.mint)
                if mint_info and mint_info.get("decimals") is not None:
                    transfer.decimals = mint_info["decimals"]
        if not transfer.mint:
            logger.warning(f"Using wSOL as default for missing mint in {instr_type} tx {signature}")
            transfer.mint = WRAPPED_SOL_MINT
        if not transfer.decimals and instr_type in ("burn", "burnChecked"):
            transfer.decimals = 0
        elif not transfer.decimals:
            logger.warning(f"Using wSOL decimals as default for missing decimals in {instr_type} tx {signature}")
            transfer.decimals = KNOWN_MINTS[WRAPPED_SOL_MINT]["decimals"]
        if not transfer.source and transfer.source_token_account:
            logger.warning(f"Could not determine source for {instr_type} tx {signature}, using token account as fallback")
            transfer.source = transfer.source_token_account
        if not transfer.destination and transfer.destination_token_account:
            tx_account_info = self.cache.get_tx_account_info(signature, transfer.destination_token_account)
            if tx_account_info and tx_account_info.get("owner"):
                transfer.destination = tx_account_info["owner"]
            else:
                if transfer.tx_status == "succeeded":
                    logger.warning(f"Could not determine destination for {instr_type} tx {signature}, using token account as fallback")
                transfer.destination = transfer.destination_token_account
        if instr_type in ("burn", "burnChecked") and not transfer.destination:
            transfer.destination = "BURN"
            transfer.destination_token_account = "BURN"
        if instr_type in ("mintTo", "mintToChecked") and not transfer.source:
            if transfer.mint:
                transfer.source = f"MINT:{transfer.mint}"
            else:
                transfer.source = "MINT"

    @staticmethod
    def _extract_mint_from_account(account_info: Dict) -> Optional[str]:
        return account_info.get("data", {}).get("parsed", {}).get("info", {}).get("mint")

class TransactionProcessor:
    """Process Solana transactions to extract token transfers"""
    def __init__(self, extractor: TokenTransferExtractor):
        self.extractor = extractor

    async def process_transaction(self, session: aiohttp.ClientSession, tx: Dict, block_number: int, block_time: int) -> List[TokenTransfer]:
        return await self.extractor.extract_token_transfers(session, tx, block_number, block_time)

    async def process_all_transactions(self, transactions: List[Dict], block_number: int, block_time: int) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            tasks = [self.process_transaction(session, tx, block_number, block_time) for tx in transactions]
            results = await asyncio.gather(*tasks)
            all_transfers = []
            for result in results:
                if result:
                    all_transfers.extend(result)
            return [asdict(transfer) for transfer in all_transfers]

async def main_async():
    block_number = 268278580
    cache = AccountCache()
    rpc_client = SolanaRpcClient(RPC_ENDPOINT, cache)
    token_processor = TokenAccountProcessor(rpc_client)
    instruction_parser = InstructionParser(cache)
    extractor = TokenTransferExtractor(rpc_client, token_processor, instruction_parser)
    transaction_processor = TransactionProcessor(extractor)
    
    async with aiohttp.ClientSession() as session:
        block_data = await rpc_client.fetch_solana_block(session, block_number)
        if not block_data or "result" not in block_data:
            logger.error("Failed to fetch block data")
            return
        block_time = block_data["result"].get("blockTime")
        transactions = block_data["result"].get("transactions", [])
        logger.info(f"Processing {len(transactions)} transactions from block {block_number}")
        start_time = time.time()
        all_transfers = await transaction_processor.process_all_transactions(transactions, block_number, block_time)
        end_time = time.time()
        logger.info(f"Processed {len(transactions)} transactions in {end_time - start_time:.2f} seconds")
        logger.info(f"Found {len(all_transfers)} token transfers")
        if all_transfers:
            with open("token_transfers_w_instructions.json", "w") as outfile:
                json.dump(all_transfers, outfile, indent=2)
            logger.info(f"Results written to token_transfers.json")

def main():
    asyncio.run(main_async())
    
if __name__ == "__main__":
    main()