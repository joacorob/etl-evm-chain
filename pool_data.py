#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yaml
import json
from web3 import Web3

# =========================
# Cargar Configuración
# =========================
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

rpc_url = config["rpc_url"]
w3 = Web3(Web3.HTTPProvider(rpc_url))

if not w3.is_connected():
    raise RuntimeError(f"Error: no se pudo conectar al nodo {rpc_url}")

# =========================
# Cargar ABIs
# =========================
with open("./abi/pool.json", "r") as f:
    pool_abi = json.load(f)

with open("./abi/token.json", "r") as f:
    token_abi = json.load(f)

# =========================
# Procesar contratos
# =========================
for contract_cfg in config["contracts"]:
    name = contract_cfg["name"]
    address = Web3.to_checksum_address(contract_cfg["address"])
    contract = w3.eth.contract(address=address, abi=pool_abi)

    try:
        token0_address = contract.functions.token0().call()
        token1_address = contract.functions.token1().call()
    except Exception as e:
        print(f"[ERROR] No se pudieron obtener tokens de {name}: {e}")
        continue

    # Crear contratos de tokens
    token0 = w3.eth.contract(address=token0_address, abi=token_abi)
    token1 = w3.eth.contract(address=token1_address, abi=token_abi)

    try:
        symbol0 = token0.functions.symbol().call()
        decimals0 = token0.functions.decimals().call()
    except Exception as e:
        symbol0, decimals0 = f"ERR: {e}", "?"

    try:
        symbol1 = token1.functions.symbol().call()
        decimals1 = token1.functions.decimals().call()
    except Exception as e:
        symbol1, decimals1 = f"ERR: {e}", "?"

    print(f"\n📌 Pool: {name}")
    print(f"  token0: {token0_address} ({symbol0}, {decimals0} decimales)")
    print(f"  token1: {token1_address} ({symbol1}, {decimals1} decimales)")
