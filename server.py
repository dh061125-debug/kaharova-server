#!/usr/bin/env python3
"""Kaharova Multiplayer Relay Server"""
import asyncio
import json
import uuid
import os
import logging
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- State ---
match_queue: list = []           # websockets waiting for random match
rooms: dict = {}                 # room_id -> {name, host_ws, guest_ws, code, game_state}
player_room: dict = {}           # id(ws) -> room_id

# --- Helpers ---
async def _send(ws, data: dict):
    try:
        if ws:
            await ws.send(json.dumps(data, ensure_ascii=False))
    except Exception as e:
        logger.debug(f"_send failed: {e}")

async def _get_opponent(ws):
    room_id = player_room.get(id(ws))
    if not room_id:
        return None
    room = rooms.get(room_id)
    if not room:
        return None
    return room["guest_ws"] if room["host_ws"] is ws else room["host_ws"]

async def _cleanup_player(ws, notify: bool = True):
    if ws in match_queue:
        match_queue.remove(ws)

    room_id = player_room.pop(id(ws), None)
    if not room_id or room_id not in rooms:
        return

    room = rooms.pop(room_id)
    opponent = room["guest_ws"] if room["host_ws"] is ws else room["host_ws"]
    if opponent:
        player_room.pop(id(opponent), None)
        if notify:
            await _send(opponent, {"type": "opponent_disconnected"})

# --- Handlers ---
async def _handle_find_match(ws):
    await _cleanup_player(ws, notify=False)
    if match_queue:
        opponent = match_queue.pop(0)
        room_id = str(uuid.uuid4())
        rooms[room_id] = {
            "name": f"Random_{room_id[:6]}",
            "host_ws": opponent,
            "guest_ws": ws,
            "code": room_id[:6].upper(),
        }
        player_room[id(opponent)] = room_id
        player_room[id(ws)] = room_id
        logger.info(f"[MATCH] room={room_id[:8]}")
        await _send(opponent, {"type": "matched", "role": "host"})
        await _send(ws,       {"type": "matched", "role": "guest"})
    else:
        match_queue.append(ws)
        await _send(ws, {"type": "waiting"})

async def _handle_cancel_match(ws):
    if ws in match_queue:
        match_queue.remove(ws)
    await _send(ws, {"type": "match_cancelled"})

async def _handle_create_room(ws, data: dict):
    await _cleanup_player(ws, notify=False)
    room_name = str(data.get("room_name", "My Room"))[:30]
    code = str(uuid.uuid4())[:6].upper()
    room_id = str(uuid.uuid4())
    rooms[room_id] = {"name": room_name, "host_ws": ws, "guest_ws": None, "code": code}
    player_room[id(ws)] = room_id
    logger.info(f"[ROOM CREATED] {room_name} code={code}")
    await _send(ws, {"type": "room_created", "code": code, "name": room_name})

async def _handle_get_rooms(ws):
    room_list = [
        {"name": r["name"], "code": r["code"]}
        for r in rooms.values()
        if r["guest_ws"] is None
    ]
    await _send(ws, {"type": "room_list", "rooms": room_list})

async def _handle_join_by_code(ws, data: dict):
    code = str(data.get("code", "")).upper().strip()
    for rid, r in list(rooms.items()):
        if r["code"] == code and r["guest_ws"] is None and r["host_ws"] is not ws:
            r["guest_ws"] = ws
            player_room[id(ws)] = rid
            logger.info(f"[ROOM JOINED] code={code}")
            await _send(ws, {"type": "room_joined", "name": r["name"]})
            await _send(r["host_ws"], {"type": "opponent_joined"})
            return
    await _send(ws, {"type": "error", "message": "방 코드를 찾을 수 없습니다."})

async def _handle_game_action(ws, data: dict):
    opponent = await _get_opponent(ws)
    if opponent:
        relay = dict(data)
        relay["type"] = "opponent_action"
        await _send(opponent, relay)

# --- Main loop ---
async def handle_client(ws):
    logger.info(f"[CONNECT] {ws.remote_address}")
    try:
        async for message in ws:
            try:
                data = json.loads(message)
                t = data.get("type", "")
                if   t == "find_match":    await _handle_find_match(ws)
                elif t == "cancel_match":  await _handle_cancel_match(ws)
                elif t == "create_room":   await _handle_create_room(ws, data)
                elif t == "get_rooms":     await _handle_get_rooms(ws)
                elif t == "join_by_code":  await _handle_join_by_code(ws, data)
                elif t == "game_action":   await _handle_game_action(ws, data)
                elif t == "ping":          await _send(ws, {"type": "pong"})
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error(f"Handler error: {e}")
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await _cleanup_player(ws)
        logger.info(f"[DISCONNECT] {ws.remote_address}")

async def main():
    port = int(os.environ.get("PORT", 8765))
    logger.info(f"Server listening on 0.0.0.0:{port}")
    async with websockets.serve(handle_client, "0.0.0.0", port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
