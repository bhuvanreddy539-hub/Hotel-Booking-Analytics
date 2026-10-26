"""Connect to the WebSocket and print the first messages (needs: pip install websockets)."""
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://localhost:8000/ws") as w:
        for _ in range(4):
            print(json.loads(await w.recv()))
asyncio.run(main())
