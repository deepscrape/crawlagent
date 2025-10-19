import asyncio
import logging

import pytest
import websockets

WS_URL = "ws://localhost:8000/api/v1/ws/events?token=eyJhbGciOiJSUzI1NiIsImtpZCI6ImU4MWYwNTJhZWYwNDBhOTdjMzlkMjY1MzgxZGU2Y2I0MzRiYzM1ZjMiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoizqDPgc6_zrrPjM-AzrfPgiDOsc69z4TPic69zrnOrM60zrfPgiIsInBpY3R1cmUiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS0vQUxWLVVqVXdqckR1c0d2RUU2dGJMbWk2eEhjWUNPclRXeHNObVp5MDVpTVZhd056OTA0X2wwTEpGbm1vQVFxRVh3Z2t4S0o2Y0pqSExvRmJlVXhybVpsZTlvZUJGNm1yOEJkVjBrZXVtQ1ZjVHhPQTlBNTlFZ29OWV9kQVBWVzVYQ09TYjBkcE83WHJfQTBsQlhqd242X3JLTDJvR2VGRmpnSGhaTkt0REtjLWpOQnJ2a1UtZkxHR1F5RnR3blZzc2dmYjc5dU1vZ1d2NE5BMEp6YzdZQTd2aXBYajlJc05fZWw1Y1NpSnNkNUVSR3lDX1BnNGoyY1hsZ3FkU0dOWHZxR1YzYnU4eGMwNDdpSzV0M2hXMVpfUnhfQl9tbzNCNlJiNXNIZnJmSC14bU9lc1J4RXRaUG5sZlFxbUlYdVdOREJfMjREb293cTZBZVpTWWVZcm1qNXl6ZkwwcmppV2hsNnFxLUZ5dGZrYWpkTFhIMW5LQWd3aWFQWjN3UTk4anlnV2xLRlZTM0R6bHVXYTlJR093QTNETGpoT3VxbXdZR05naktmNGYtRGtENWFSOHdQMkdOQ0paTFc2NHNzM1pWWHF3LTVGWHlOOVdWbzdIMi13OTNKdkZ6YXFIN1lOX2diZ0hTODk3M0V0THdaNUJXTlRaTXZ0NHU4Y253bFJnZEVuRHhXemk1S0p2bzh3NFplZFFvcEFtdktkaGFQU1JJck1UT05sNGd6ckdNZkZhNXdlaE9ydXVZcVd4blJEdktSZ0NmalRiZ0pLdC1TamtEUWppc0JMSzduZ296bjBVRkM0bnRUUjNkNDZJUE1WaVVEcks1cG44MXJNVGtRbWxfdFA0OWNMN2FIRXVabFdkS01yR3k0ZDF5c0VZeW5qS1pybGNWQ1BpVE9HX05fdlB4NVR2dG9BN1VEbk1iamxJa0xaUVBfbkQySHpWRS14R0tjejZIYWEwVXRWckhfQ0hUTmdBbENNc1BJTHFuQVNtYXlkN2tna1RLbjQ0LUxFTVo0dTFfb0FjSkpXc1FyaFpTSndnWDV1ZWh3RDVqdDE0MlhYMW93anp2X1JJSklQbDdxSWJYWWFOSmtKOElqNnd5R2t2UkFlWm5EX0J0Q1VVT1VnY2JoZC1NVm4yRWxJQml5U2V3TWZvb1gweTR2VkxQQzVIQjBMaHhzcXFOa01IWmk2dU5reWlJMW5mb0EyU3I5RHpnSEVWUFNybFNwbHMxTmhSU3ZCNmI1Nk1NOFpmUnRZQlhNLVo3MEgyaHJHUEpKWVdRQ3hQa3NFa0ZCaWNQd1BLVmxoSzRFdUtBPXM5Ni1jIiwiaXNzIjoiaHR0cHM6Ly9zZWN1cmV0b2tlbi5nb29nbGUuY29tL2xpYm5ldC1kNzZkYiIsImF1ZCI6ImxpYm5ldC1kNzZkYiIsImF1dGhfdGltZSI6MTc1ODQ2NDA4NiwidXNlcl9pZCI6IlNmVTc5RjA5dlhZdTBEcHFJWTVVTHRXTG9GWTIiLCJzdWIiOiJTZlU3OUYwOXZYWXUwRHBxSVk1VUx0V0xvRlkyIiwiaWF0IjoxNzU5MjIxMDI0LCJleHAiOjE3NTkyMjQ2MjQsImVtYWlsIjoicHJva29waXMxMjNAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZ29vZ2xlLmNvbSI6WyIxMDE3OTQxNDQ1MTIzNDkwMjc2MTUiXSwiZ2l0aHViLmNvbSI6WyIyMDA5ODIwNiJdLCJlbWFpbCI6WyJwcm9rb3BpczEyM0BnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJnb29nbGUuY29tIn19.YgeSSg6cg2zysajsJya5mNohNov-Btp_mUzKQZ5yff_IQXbuOpI0NEoeveKFLVLprRKhKm6xiH6WoM6wl6G-9hMnAzhngXVhJsV9W4GVGF23mAHXkmXFJQjGY4GNm25hefeecrh9TKqSQEbIJWcHcwvR3lRs1emLTWUBaXf9gRRIhiCiFUNCWixguCK6SNj0xXyOLKNaPTxN_C7QHIFbI8SkUuX5uz25-JbgJ0qeVX7V5H6D-kTI28f7bjRAowWpni4zsO9T40lHmnKODY9FNHYTXYEwstcizwRBY7dF5JYURGAwHcl0uY4i5b7Lu7AMF2a31D9GpCJAU-vGIZJhTg"
logger = logging.getLogger(__name__)
@pytest.mark.asyncio
async def test_websocket_events():
    async with websockets.connect(WS_URL) as websocket:
        # Wait for a message from the server
        msg = await websocket.recv()
        logger.info(f"Received message: {msg}")
        if isinstance(msg, bytes):
            msg_str = msg.decode("utf-8")
        elif isinstance(msg, (bytearray, memoryview)):
            msg_str = bytes(msg).decode("utf-8")
        else:
            msg_str = str(msg)
        assert "server event" in msg_str
        # Optionally, send a message to the server and check response
        # await websocket.send("ping")
        # response = await websocket.recv()
        # assert response == "pong"  # If your server echoes or responds

@pytest.mark.asyncio
async def test_websocket_no_close():
    websocket = await websockets.connect(WS_URL)
    try:
        # Wait for a message from the server
        msg = await websocket.recv()
        if isinstance(msg, bytes):
            msg_str = msg.decode("utf-8")
        elif isinstance(msg, (bytearray, memoryview)):
            msg_str = bytes(msg).decode("utf-8")
        else:
            msg_str = str(msg)
        assert "server event" in msg_str
        # Optionally, send a message to the server and check response
        # await websocket.send("ping")
        # response = await websocket.recv()
        # assert response == "pong"  # If your server echoes or responds
        await asyncio.sleep(120)  # Keep the connection alive for 1 second
    finally:
        await websocket.close()