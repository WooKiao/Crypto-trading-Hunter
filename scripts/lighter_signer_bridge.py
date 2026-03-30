#!/usr/bin/env python3

import asyncio
import json
import os
import sys


def emit(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def require_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"missing {name}")
    return value


async def place_market_order(client, payload):
    created_tx, response, err = await client.create_market_order(
        market_index=int(payload["market_index"]),
        client_order_index=int(payload["client_order_index"]),
        base_amount=int(payload["base_amount"]),
        avg_execution_price=int(payload["price"]),
        is_ask=bool(payload["is_ask"]),
        reduce_only=bool(payload["reduce_only"]),
    )
    if err is not None:
        raise RuntimeError(str(err))

    response_payload = response.to_dict() if hasattr(response, "to_dict") else {
        "code": getattr(response, "code", None),
        "message": getattr(response, "message", None),
        "tx_hash": getattr(response, "tx_hash", None),
        "predicted_execution_time_ms": getattr(response, "predicted_execution_time_ms", None),
        "volume_quota_remaining": getattr(response, "volume_quota_remaining", None),
    }
    return {
        "response": response_payload,
        "tx": json.loads(created_tx.to_json()) if hasattr(created_tx, "to_json") else None,
    }


async def main():
    try:
        import lighter
    except Exception as exc:
        emit({"ready": False, "error": f"failed to import lighter SDK: {exc}"})
        return 1

    try:
        base_url = os.getenv("LIGHTER_API_BASE_URL", "https://mainnet.zklighter.elliot.ai")
        account_index = int(require_env("LIGHTER_ACCOUNT_INDEX"))
        api_key_index = int(require_env("LIGHTER_API_KEY_INDEX"))
        api_private_key = require_env("LIGHTER_API_PRIVATE_KEY")
        client = lighter.SignerClient(
            url=base_url,
            account_index=account_index,
            api_private_keys={api_key_index: api_private_key},
        )
        check_error = client.check_client()
        if check_error is not None:
            raise RuntimeError(str(check_error).strip())
    except Exception as exc:
        emit({"ready": False, "error": str(exc)})
        return 1

    emit({"ready": True})

    try:
        for raw_line in sys.stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                result = await place_market_order(client, payload)
                emit({"ok": True, "response": result["response"], "tx": result["tx"]})
            except Exception as exc:
                emit({"ok": False, "error": str(exc).strip()})
    finally:
        await client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
